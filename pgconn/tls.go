package pgconn

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"
)

// tlsConfigRegistry is a collection of tls.Config.
type tlsConfigRegistry map[string]*tls.Config

// tlsCACertPoolRegistry is a collection of x509.CertPool.
type tlsCACertPoolRegistry map[string]*x509.CertPool

// tlsRegistry is a collection of tls.Config and CA certificate pools.
type tlsRegistry struct {
	configs     tlsConfigRegistry
	caCertPools tlsCACertPoolRegistry
}

// findConfig tries to find a tls.Config from the registry and clones if found, or returns nil otherwise.
func (r *tlsRegistry) findConfig(key string) *tls.Config {
	config, ok := r.configs[key]
	if !ok {
		return nil
	}

	return config.Clone()
}

// setConfig adds the tls.Config to the registry.
func (r *tlsRegistry) setConfig(key string, config *tls.Config) {
	if r.configs == nil {
		r.configs = make(tlsConfigRegistry)
	}

	r.configs[key] = config
}

// deleteConfig removes the tls.Config from the registry.
func (r *tlsRegistry) deleteConfig(key string) {
	if r.configs == nil {
		return
	}

	delete(r.configs, key)
}

// findCACertPool tries to find a x509.CertPool from the registry and clones if found, or returns nil otherwise.
func (r *tlsRegistry) findCACertPool(key string) *x509.CertPool {
	certPool, ok := r.caCertPools[key]
	if !ok {
		return nil
	}

	return certPool.Clone()
}

// setCertPool adds the x509.CertPool to the registry.
func (r *tlsRegistry) setCertPool(key string, certPool *x509.CertPool) {
	if r.caCertPools == nil {
		r.caCertPools = make(tlsCACertPoolRegistry)
	}

	r.caCertPools[key] = certPool
}

// deleteCertPool removes the x509.CertPool from the registry.
func (r *tlsRegistry) deleteCertPool(key string) {
	if r.caCertPools == nil {
		return
	}

	delete(r.caCertPools, key)
}

type tlsRegistryWithMutex struct {
	tlsRegistry
	mutex sync.RWMutex
}

var globalTLSRegistry tlsRegistryWithMutex

// findTLSConfig tries to find a tls.Config from the registry and clones if found, or returns nil otherwise.
func findTLSConfig(key string) *tls.Config {
	globalTLSRegistry.mutex.RLock()
	defer globalTLSRegistry.mutex.RUnlock()

	return globalTLSRegistry.findConfig(key)
}

// RegisterTLSConfig registers a custom tls.Config to be used with sql.Open.
// Use the key as a value in the DSN where sslmode=value.
func RegisterTLSConfig(key string, config *tls.Config) error {
	switch key {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return fmt.Errorf("key '%s' is reserved", key)
	}

	globalTLSRegistry.mutex.Lock()
	defer globalTLSRegistry.mutex.Unlock()

	globalTLSRegistry.setConfig(key, config)

	return nil
}

// DeregisterTLSConfig removes the tls.Config associated with key.
func DeregisterTLSConfig(key string) {
	globalTLSRegistry.mutex.Lock()
	defer globalTLSRegistry.mutex.Unlock()

	globalTLSRegistry.deleteConfig(key)
}

// findTLSCACertPool tries to find a x509.CertPool from the registry and clones if found, or returns nil otherwise.
func findTLSCACertPool(key string) *x509.CertPool {
	globalTLSRegistry.mutex.RLock()
	defer globalTLSRegistry.mutex.RUnlock()

	return globalTLSRegistry.findCACertPool(key)
}

// RegisterTLSCACertPool registers a custom x509.CertPool to be used with sql.Open.
// Use the key as a value in the DSN where sslrootcert=value.
func RegisterTLSCACertPool(key string, certPool *x509.CertPool) error {
	globalTLSRegistry.mutex.Lock()
	defer globalTLSRegistry.mutex.Unlock()

	globalTLSRegistry.setCertPool(key, certPool)

	return nil
}

// DeregisterTLSCACertPool removes the x509.CertPool associated with key.
func DeregisterTLSCACertPool(key string) {
	globalTLSRegistry.mutex.Lock()
	defer globalTLSRegistry.mutex.Unlock()

	globalTLSRegistry.deleteCertPool(key)
}
