package relay

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

type Policy struct{ AllowLocalDemo bool }

// Local demo mode is limited to literal loopback addresses, not arbitrary LAN IPs.
func (p Policy) Validate(raw string) error {
	if len(raw) > 2048 {
		return errors.New("destination URL is too long")
	}
	u, e := url.Parse(raw)
	if e != nil {
		return errors.New("invalid URL")
	}
	if u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return errors.New("URL needs a host and must not include credentials or a fragment")
	}
	if p.AllowLocalDemo {
		if a, e := netip.ParseAddr(u.Hostname()); e == nil && a.IsLoopback() && (u.Scheme == "http" || u.Scheme == "https") {
			return nil
		}
	}
	if u.Scheme != "https" {
		return errors.New("only HTTPS destinations are allowed")
	}
	if u.Port() != "" && u.Port() != "443" {
		return errors.New("only port 443 is allowed")
	}
	if a, e := netip.ParseAddr(u.Hostname()); e == nil && !publicIP(a) {
		return errors.New("destination must be a public address")
	}
	h := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if h == "localhost" || strings.HasSuffix(h, ".localhost") || strings.HasSuffix(h, ".local") {
		return errors.New("local hostnames are forbidden")
	}
	return nil
}

var deniedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"), netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("192.168.0.0/16"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/4"), netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2001::/32"), netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("64:ff9b::/96"), netip.MustParsePrefix("64:ff9b:1::/48"),
}

func publicIP(a netip.Addr) bool {
	a = a.Unmap()
	// Restrict IPv6 to the allocated global-unicast block. Discard-only,
	// translation, local-use and future allocations are not webhook targets.
	if a.Is6() && !netip.MustParsePrefix("2000::/3").Contains(a) {
		return false
	}
	if netip.MustParsePrefix("2001::/23").Contains(a) || netip.MustParsePrefix("3fff::/20").Contains(a) || netip.MustParsePrefix("192.88.99.0/24").Contains(a) {
		return false
	}
	if !a.IsGlobalUnicast() || a.IsPrivate() || a.IsLoopback() || a.IsLinkLocalUnicast() {
		return false
	}
	for _, p := range deniedPrefixes {
		if p.Contains(a) {
			return false
		}
	}
	return true
}

// DNS is resolved and checked at connection time; the dial is pinned to that
// checked IP. Redirects and environment proxies are deliberately disabled.
func NewHTTPClient(p Policy, timeout time.Duration) *http.Client {
	d := net.Dialer{Timeout: timeout}
	tr := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, DisableKeepAlives: true, ResponseHeaderTimeout: timeout}
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, e
		}
		ips, e := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if e != nil {
			return nil, e
		}
		if len(ips) == 0 {
			return nil, errors.New("no DNS addresses")
		}
		literal, literalErr := netip.ParseAddr(host)
		for _, a := range ips {
			if !(publicIP(a) || (p.AllowLocalDemo && literalErr == nil && a.Unmap().IsLoopback() && literal.Unmap() == a.Unmap())) {
				return nil, errors.New("DNS resolved to a forbidden address")
			}
		}
		return d.DialContext(ctx, network, net.JoinHostPort(ips[0].String(), port))
	}
	return &http.Client{Timeout: timeout, Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func Signature(secret, timestamp, id string, payload []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(h, "%s.%s.", timestamp, id)
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}
func VerifySignature(secret, timestamp, id string, payload []byte, signature string, now time.Time, tolerance time.Duration) bool {
	t, e := time.Parse(time.RFC3339, timestamp)
	if e != nil || now.Sub(t) > tolerance || t.Sub(now) > tolerance {
		return false
	}
	got, e := hex.DecodeString(signature)
	if e != nil {
		return false
	}
	want, _ := hex.DecodeString(Signature(secret, timestamp, id, payload))
	return hmac.Equal(got, want)
}
