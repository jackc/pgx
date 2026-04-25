// File export_test exports some methods for better testing.

package pgconn

import "context"

// BuildConnectOneConfigsFromSRV exposes the internal SRV resolution logic for
// white-box testing. It returns the ordered list of (network, address,
// originalHostname) tuples that pgconn would attempt to connect to, without
// actually opening any TCP connections.
func BuildConnectOneConfigsFromSRV(ctx context.Context, config *Config) ([]ResolvedSRVTarget, error) {
	configs, errs := buildConnectOneConfigsFromSRV(ctx, config)
	if len(errs) > 0 {
		return nil, errs[0]
	}
	targets := make([]ResolvedSRVTarget, len(configs))
	for i, c := range configs {
		targets[i] = ResolvedSRVTarget{
			Network:          c.network,
			Address:          c.address,
			OriginalHostname: c.originalHostname,
		}
	}
	return targets, nil
}

// ResolvedSRVTarget holds the resolved address information for one SRV target.
type ResolvedSRVTarget struct {
	Network          string // "tcp"
	Address          string // "host:port"
	OriginalHostname string // SRV target after trimming trailing dot
}
