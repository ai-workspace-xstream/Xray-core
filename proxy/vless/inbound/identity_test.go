package inbound

import "testing"

func TestProxyIdentityRefIsStableAndCredentialScoped(t *testing.T) {
	credential := "A55A9F3A-5D63-4B99-9C3A-9DFEB6D57DA2"
	got := proxyIdentityRef(credential)
	if got == "" || got != proxyIdentityRef("  "+credential+"  ") {
		t.Fatalf("identity reference is not stable after canonicalization: %q", got)
	}
	if got == credential || got == proxyIdentityRef("different-credential") {
		t.Fatal("identity reference must be opaque and credential-specific")
	}
}
