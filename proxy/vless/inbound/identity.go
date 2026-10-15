package inbound

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const proxyIdentityDomain = "xconnect.proxy_identity_ref.v1"

// proxyIdentityRef produces a stable pseudonym from Xray's authenticated
// credential. The credential is high entropy and serves as the per-identity
// HMAC key, so no fleet-wide telemetry secret is required. Never attach the
// credential or configured VLESS email to a span.
func proxyIdentityRef(credential string) string {
	key := strings.ToLower(strings.TrimSpace(credential))
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write([]byte(proxyIdentityDomain))
	return hex.EncodeToString(mac.Sum(nil))
}
