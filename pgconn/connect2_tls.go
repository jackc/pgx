package pgconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
)

func negotiateTLS(ctx context.Context, conn net.Conn, config *tls.Config, negotiation string, allowPlain bool, phase *string) (net.Conn, error) {
	return negotiateTLSWithProvider(ctx, conn, func(context.Context) (*tls.Config, error) { return config, nil }, negotiation, allowPlain, phase)
}

func negotiateTLSWithProvider(ctx context.Context, conn net.Conn, load func(context.Context) (*tls.Config, error), negotiation string, allowPlain bool, phase *string) (net.Conn, error) {
	if negotiation != "direct" {
		if err := binary.Write(conn, binary.BigEndian, []int32{8, 80877103}); err != nil {
			return nil, err
		}
		var b [1]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return nil, err
		}
		if b[0] == 'N' && allowPlain {
			return conn, nil
		}
		if b[0] != 'S' {
			return nil, errors.New("server refused or sent invalid TLS negotiation response")
		}
	}
	*phase = "tls-handshake"
	config, err := load(ctx)
	if err != nil {
		return nil, err
	}
	tc := tls.Client(conn, config)
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, err
	}
	if negotiation == "direct" && tc.ConnectionState().NegotiatedProtocol != "postgresql" {
		return nil, errors.New("direct TLS requires postgresql ALPN")
	}
	return tc, nil
}

func (s *connectSettings) tlsConfig(ctx context.Context, info ConnectionInfo) (*tls.Config, error) {
	v := maps.Clone(s.values)
	var material TLSMaterial
	if s.options.TLSProvider != nil {
		var err error
		material, err = s.options.TLSProvider(ctx, info)
		if err != nil {
			return nil, err
		}
		if material.RootCAs != nil {
			delete(v, "sslrootcert")
		}
		if material.Certificates != nil {
			delete(v, "sslcert")
			delete(v, "sslkey")
		}
	}
	// File loading is shared, but verification is rebuilt below. This avoids
	// inheriting legacy system-root defaulting and SNI/identity coupling.
	mode := s.values["sslmode"]
	v["sslmode"] = "prefer"
	v["sslnegotiation"] = "postgres"
	host := info.Host
	if host == "" {
		host = s.hostAddress(info)
	}
	opts := ParseConfigOptions{}
	if s.options.GetSSLPassword != nil {
		opts.GetSSLPassword = func(context.Context) string { return s.options.GetSSLPassword(ctx) }
	}
	configs, err := configTLS(v, host, opts)
	if err != nil {
		return nil, err
	}
	c := configs[0]
	if material.RootCAs != nil {
		c.RootCAs = material.RootCAs.Clone()
	}
	if material.Certificates != nil {
		c.Certificates = material.Certificates
	}
	c.InsecureSkipVerify = true
	c.VerifyPeerCertificate = nil
	c.ServerName = ""
	if s.values["sslsni"] == "1" && net.ParseIP(host) == nil {
		c.ServerName = host
	}
	if s.values["sslnegotiation"] == "direct" {
		c.NextProtos = []string{"postgresql"}
	}
	verify := mode == "verify-full" || mode == "verify-ca" || mode == "require" && c.RootCAs != nil
	if (mode == "verify-full" || mode == "verify-ca") && c.RootCAs == nil {
		return nil, errors.New("sslmode requires a root certificate source")
	}
	if verify {
		c.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("missing server certificate")
			}
			intermediates := x509.NewCertPool()
			for _, cert := range cs.PeerCertificates[1:] {
				intermediates.AddCert(cert)
			}
			vo := x509.VerifyOptions{Roots: c.RootCAs, Intermediates: intermediates}
			if mode == "verify-full" {
				vo.DNSName = host
			}
			if _, err := cs.PeerCertificates[0].Verify(vo); err != nil {
				return err
			}
			if material.VerifyConnection != nil {
				return material.VerifyConnection(cs)
			}
			return nil
		}
	} else {
		c.VerifyConnection = material.VerifyConnection
	}
	for k, dst := range map[string]*uint16{"ssl_min_protocol_version": &c.MinVersion, "ssl_max_protocol_version": &c.MaxVersion} {
		if t := s.values[k]; t != "" {
			ver, ok := map[string]uint16{"TLSv1": tls.VersionTLS10, "TLSv1.1": tls.VersionTLS11, "TLSv1.2": tls.VersionTLS12, "TLSv1.3": tls.VersionTLS13}[t]
			if !ok {
				return nil, fmt.Errorf("invalid %s", k)
			}
			*dst = ver
		}
	}
	if c.MaxVersion != 0 && c.MinVersion > c.MaxVersion {
		return nil, errors.New("TLS minimum exceeds maximum")
	}
	return c, nil
}

func (s *connectSettings) hostAddress(info ConnectionInfo) string {
	h, _, _ := net.SplitHostPort(info.Address)
	return h
}
