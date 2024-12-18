// Generates a CA, server certificate, and encrypted client certificate for testing pgx.

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

func main() {
	// Create the CA
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "pgx-root-ca",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(20, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		panic(err)
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		panic(err)
	}

	err = writePrivateKey("ca.key", caKey)
	if err != nil {
		panic(err)
	}

	err = writeCertificate("ca.pem", caBytes)
	if err != nil {
		panic(err)
	}

	// Create a server certificate signed by the CA for localhost.
	serverCert := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(20, 0, 0),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature,
	}

	serverCertPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	serverBytes, err := x509.CreateCertificate(rand.Reader, serverCert, ca, &serverCertPrivKey.PublicKey, caKey)
	if err != nil {
		panic(err)
	}

	err = writePrivateKey("localhost.key", serverCertPrivKey)
	if err != nil {
		panic(err)
	}

	err = writeCertificate("localhost.crt", serverBytes)
	if err != nil {
		panic(err)
	}

	// Create a client certificate signed by the CA and encrypted.
	clientCert := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName: "pgx_sslcert",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(20, 0, 0),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature,
	}

	clientCertPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	clientBytes, err := x509.CreateCertificate(rand.Reader, clientCert, ca, &clientCertPrivKey.PublicKey, caKey)
	if err != nil {
		panic(err)
	}

	err = writeEncryptedPrivateKey("pgx_sslcert.key", clientCertPrivKey, "certpw")
	if err != nil {
		panic(err)
	}

	err = writeCertificate("pgx_sslcert.crt", clientBytes)
	if err != nil {
		panic(err)
	}
}

func writePrivateKey(path string, privateKey *rsa.PrivateKey) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("writePrivateKey: %w", err)
	}

	err = pem.Encode(file, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	if err != nil {
		return fmt.Errorf("writePrivateKey: %w", err)
	}

	err = file.Close()
	if err != nil {
		return fmt.Errorf("writePrivateKey: %w", err)
	}

	return nil
}

func writeEncryptedPrivateKey(path string, privateKey *rsa.PrivateKey, password string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("writeEncryptedPrivateKey: %w", err)
	}

	block, err := x509.EncryptPEMBlock(rand.Reader, "CERTIFICATE", x509.MarshalPKCS1PrivateKey(privateKey), []byte(password), x509.PEMCipher3DES)
	if err != nil {
		return fmt.Errorf("writeEncryptedPrivateKey: %w", err)
	}

	err = pem.Encode(file, block)
	if err != nil {
		return fmt.Errorf("writeEncryptedPrivateKey: %w", err)
	}

	err = file.Close()
	if err != nil {
		return fmt.Errorf("writeEncryptedPrivateKey: %w", err)
	}

	return nil

}

func writeCertificate(path string, certBytes []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("writeCertificate: %w", err)
	}

	err = pem.Encode(file, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})
	if err != nil {
		return fmt.Errorf("writeCertificate: %w", err)
	}

	err = file.Close()
	if err != nil {
		return fmt.Errorf("writeCertificate: %w", err)
	}

	return nil
}
