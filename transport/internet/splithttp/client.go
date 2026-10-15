package splithttp

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/signal/done"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// interface to abstract between use of browser dialer, vs net/http
type DialerClient interface {
	IsClosed() bool

	// ctx, url, sessionId, body, uploadOnly
	OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error)

	// ctx, url, sessionId, seqStr, body, contentLength
	PostPacket(context.Context, string, string, string, buf.MultiBuffer) error
}

// implements splithttp.DialerClient in terms of direct network connections
type DefaultDialerClient struct {
	transportConfig *Config
	client          *http.Client
	closed          atomic.Bool
	httpVersion     string
	// pool of net.Conn, created using dialUploadConn
	uploadRawPool  *sync.Pool
	dialUploadConn func(ctxInner context.Context) (net.Conn, error)
}

func (c *DefaultDialerClient) IsClosed() bool {
	return c.closed.Load()
}

func (c *DefaultDialerClient) OpenStream(ctx context.Context, url string, sessionId string, body io.Reader, uploadOnly bool) (wrc io.ReadCloser, remoteAddr, localAddr net.Addr, err error) {
	tracer := otel.Tracer("github.com/xtls/xray-core/transport/internet/splithttp")
	ctx, connectSpan := tracer.Start(ctx, "xconnect.client.xhttp.connect", trace.WithSpanKind(trace.SpanKindClient))
	connectSpan.SetAttributes(attribute.String("xconnect.transport", "xhttp"))
	defer connectSpan.End()
	_, payloadSpan := tracer.Start(ctx, "xconnect.client.xhttp.first_payload_wait", trace.WithSpanKind(trace.SpanKindClient))
	payloadStarted := time.Now()
	var payloadOnce sync.Once
	var phaseMu sync.Mutex
	phaseStart := make(map[string]time.Time)
	phaseDuration := make(map[string]time.Duration)
	phaseBegin := func(name string) {
		phaseMu.Lock()
		phaseStart[name] = time.Now()
		phaseMu.Unlock()
	}
	phaseEnd := func(name string) {
		phaseMu.Lock()
		if started, ok := phaseStart[name]; ok {
			phaseDuration[name] = time.Since(started)
			delete(phaseStart, name)
		}
		phaseMu.Unlock()
	}
	finishPayload := func(outcome string, size int, readErr error) {
		payloadOnce.Do(func() {
			payloadSpan.SetAttributes(
				attribute.String("xconnect.first_payload.outcome", outcome),
				attribute.Int("xconnect.first_payload.bytes", size),
				attribute.Int64("xconnect.first_payload.wait_ms", time.Since(payloadStarted).Milliseconds()),
			)
			if readErr != nil && size == 0 {
				payloadSpan.RecordError(fmt.Errorf("first payload read failed"))
				payloadSpan.SetStatus(codes.Error, "first payload read failed")
			}
			payloadSpan.End()
		})
	}
	// this is done when the TCP/UDP connection to the server was established,
	// and we can unblock the Dial function and print correct net addresses in
	// logs
	gotConn := done.New()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { phaseBegin("dns") },
		DNSDone:  func(httptrace.DNSDoneInfo) { phaseEnd("dns") },
		ConnectStart: func(_, _ string) {
			phaseBegin("tcp_connect")
		},
		ConnectDone:       func(_, _ string, _ error) { phaseEnd("tcp_connect") },
		TLSHandshakeStart: func() { phaseBegin("tls_handshake") },
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			phaseEnd("tls_handshake")
			if state.NegotiatedProtocol != "" {
				connectSpan.SetAttributes(attribute.String("network.protocol.version", state.NegotiatedProtocol))
			}
			if err != nil {
				connectSpan.AddEvent("tls.handshake.error")
			}
		},
		GotConn: func(connInfo httptrace.GotConnInfo) {
			remoteAddr = connInfo.Conn.RemoteAddr()
			localAddr = connInfo.Conn.LocalAddr()
			connectSpan.SetAttributes(attribute.Bool("network.connection.reused", connInfo.Reused))
			phaseMu.Lock()
			for name, duration := range phaseDuration {
				connectSpan.SetAttributes(attribute.Int64("xconnect.client.xhttp."+name+"_duration_ms", duration.Milliseconds()))
			}
			phaseMu.Unlock()
			gotConn.Close()
		},
	})

	method := "GET" // stream-down
	if body != nil {
		method = c.transportConfig.GetNormalizedUplinkHTTPMethod() // stream-up/one
	}
	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), method, url, body)
	if err != nil {
		connectSpan.RecordError(fmt.Errorf("XHTTP request construction failed"))
		connectSpan.SetStatus(codes.Error, "XHTTP request construction failed")
		errors.LogInfoInner(ctx, err, "failed to create HTTP request for "+url)
		return nil, nil, nil, err
	}
	c.transportConfig.FillStreamRequest(req, sessionId, "")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	wrc = &WaitReadCloser{wait: done.New()}
	go func() {
		resp, err := c.client.Do(req)
		if err != nil {
			connectSpan.RecordError(fmt.Errorf("XHTTP transport connection failed"))
			connectSpan.SetStatus(codes.Error, "XHTTP transport connection failed")
			finishPayload("error", 0, err)
			if !uploadOnly { // stream-down is enough
				c.closed.Store(true)
				errors.LogInfoInner(ctx, err, "failed to "+method+" "+url)
			}
			gotConn.Close()
			common.Close(body)
			wrc.Close()
			return
		}
		if resp.StatusCode != 200 && !uploadOnly {
			errors.LogInfo(ctx, "unexpected status ", resp.StatusCode)
		}
		payloadSpan.SetAttributes(
			attribute.Int("http.response.status_code", resp.StatusCode),
			attribute.String("network.protocol.version", resp.Proto),
		)
		if resp.StatusCode != 200 || uploadOnly { // stream-up
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close() // if it is called immediately, the upload will be interrupted also
			if resp.StatusCode != 200 {
				finishPayload("http_error", 0, fmt.Errorf("XHTTP upstream returned status %d", resp.StatusCode))
			} else {
				finishPayload("no_payload_expected", 0, nil)
			}
			common.Close(body)
			wrc.Close()
			return
		}
		wrc.(*WaitReadCloser).SetObserved(resp.Body, finishPayload)
	}()

	<-gotConn.Wait()
	return
}

func (c *DefaultDialerClient) PostPacket(ctx context.Context, url string, sessionId string, seqStr string, payload buf.MultiBuffer) error {
	method := c.transportConfig.GetNormalizedUplinkHTTPMethod()
	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), method, url, nil)
	if err != nil {
		return err
	}
	c.transportConfig.FillPacketRequest(req, sessionId, seqStr, payload)

	if c.httpVersion != "1.1" {
		resp, err := c.client.Do(req)
		if err != nil {
			c.closed.Store(true)
			return err
		}

		io.Copy(io.Discard, resp.Body)
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return errors.New("bad status code:", resp.Status)
		}
	} else {
		// stringify the entire HTTP/1.1 request so it can be
		// safely retried. if instead req.Write is called multiple
		// times, the body is already drained after the first
		// request
		requestBuff := new(bytes.Buffer)
		requestBuff.Grow(512 + int(req.ContentLength))
		common.Must(req.Write(requestBuff))

		var uploadConn any
		var h1UploadConn *H1Conn

		for {
			uploadConn = c.uploadRawPool.Get()
			newConnection := uploadConn == nil
			if newConnection {
				newConn, err := c.dialUploadConn(context.WithoutCancel(ctx))
				if err != nil {
					return err
				}
				h1UploadConn = NewH1Conn(newConn)
				uploadConn = h1UploadConn
			} else {
				h1UploadConn = uploadConn.(*H1Conn)

				// TODO: Replace 0 here with a config value later
				// Or add some other condition for optimization purposes
				if h1UploadConn.UnreadedResponsesCount > 0 {
					resp, err := http.ReadResponse(h1UploadConn.RespBufReader, req)
					if err != nil {
						c.closed.Store(true)
						return fmt.Errorf("error while reading response: %s", err.Error())
					}
					io.Copy(io.Discard, resp.Body)
					defer resp.Body.Close()
					if resp.StatusCode != 200 {
						return fmt.Errorf("got non-200 error response code: %d", resp.StatusCode)
					}
				}
			}

			_, err := h1UploadConn.Write(requestBuff.Bytes())
			// if the write failed, we try another connection from
			// the pool, until the write on a new connection fails.
			// failed writes to a pooled connection are normal when
			// the connection has been closed in the meantime.
			if err == nil {
				break
			} else if newConnection {
				return err
			}
		}

		c.uploadRawPool.Put(uploadConn)
	}

	return nil
}

// HTTP/1.1 and HTTP/2 will close itself, we only handle HTTP/3 here
func (c *DefaultDialerClient) Close() error {
	transport := c.client.Transport
	if h3Transport, ok := transport.(*http3.Transport); ok {
		h3Transport.Close()
	}
	return nil
}

type WaitReadCloser struct {
	wait          *done.Instance
	reader        atomic.Pointer[io.ReadCloser]
	finishPayload func(string, int, error)
}

func (w *WaitReadCloser) SetObserved(rc io.ReadCloser, finish func(string, int, error)) {
	w.finishPayload = finish
	w.Set(&firstReadObserver{ReadCloser: rc, finish: finish})
}

func (w *WaitReadCloser) Set(rc io.ReadCloser) {
	w.reader.Store(&rc)
	if w.wait.Done() {
		if p := w.reader.Swap(nil); p != nil {
			(*p).Close()
		}
	}
	w.wait.Close()
}

func (w *WaitReadCloser) Read(b []byte) (int, error) {
	rc := w.reader.Load()
	if rc == nil {
		<-w.wait.Wait()
		if rc = w.reader.Load(); rc == nil {
			return 0, io.ErrClosedPipe
		}
	}
	n, err := (*rc).Read(b)
	if observer, ok := (*rc).(*firstReadObserver); ok && (n > 0 || err != nil) {
		observer.once.Do(func() {
			outcome := "received"
			if n == 0 {
				outcome = "closed_without_payload"
			}
			observer.finish(outcome, n, err)
		})
	}
	return n, err
}

func (w *WaitReadCloser) Close() error {
	w.wait.Close()
	if w.finishPayload != nil {
		w.finishPayload("closed_before_payload", 0, nil)
	}
	if p := w.reader.Swap(nil); p != nil {
		return (*p).Close()
	}
	return nil
}

type firstReadObserver struct {
	io.ReadCloser
	finish func(string, int, error)
	once   sync.Once
}
