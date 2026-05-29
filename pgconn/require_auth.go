package pgconn

import (
	"fmt"
	"strings"
)

// authMethod is one of the method keywords accepted by libpq's require_auth.
type authMethod uint8

const (
	authMethodPassword authMethod = iota
	authMethodMD5
	authMethodGSS
	authMethodSSPI
	authMethodSCRAMSHA256
	authMethodOAuth
	authMethodNone
	authMethodCount
)

var authMethodNames = [authMethodCount]string{
	authMethodPassword:    "password",
	authMethodMD5:         "md5",
	authMethodGSS:         "gss",
	authMethodSSPI:        "sspi",
	authMethodSCRAMSHA256: "scram-sha-256",
	authMethodOAuth:       "oauth",
	authMethodNone:        "none",
}

// requireAuth is the parsed form of the require_auth connection parameter. It mirrors libpq's
// auth_required / allowed_auth_methods bookkeeping (see fe-connect.c, conn->allowed_auth_methods).
type requireAuth struct {
	// raw is the original parameter value, used in error messages.
	raw string

	// authRequired is true when the server must complete an authentication exchange before sending
	// AuthenticationOk. It is false when the parameter is unset, fully negated, or "none" is in the
	// allowed set.
	authRequired bool

	// allowed is a bitmask of permitted authMethod values.
	allowed uint8
}

func (ra requireAuth) allows(m authMethod) bool {
	return ra.allowed&(1<<m) != 0
}

// check returns an error if method m is not permitted by the policy. The reason is phrased to
// follow libpq's "authentication method requirement \"%s\" failed: %s" form so users migrating
// from libpq see familiar diagnostics.
func (ra requireAuth) check(m authMethod) error {
	if ra.allows(m) {
		return nil
	}
	var reason string
	if m == authMethodNone {
		reason = "server did not complete authentication"
	} else {
		reason = fmt.Sprintf("server requested %s authentication", authMethodNames[m])
	}
	return fmt.Errorf("authentication method requirement %q failed: %s", ra.raw, reason)
}

// parseRequireAuth parses the require_auth connection parameter with libpq-compatible semantics:
// a comma-separated list of method names, optionally each prefixed with "!" to negate. Negated and
// non-negated entries cannot be mixed; duplicate entries are rejected. An empty string yields a
// permissive policy (all methods allowed, no authentication required).
func parseRequireAuth(s string) (requireAuth, error) {
	ra := requireAuth{raw: s}

	if s == "" {
		ra.allowed = 1<<authMethodCount - 1
		return ra, nil
	}

	first := true
	negated := false
	for part := range strings.SplitSeq(s, ",") {
		method := strings.TrimSpace(part)

		neg := strings.HasPrefix(method, "!")
		if neg {
			method = method[1:]
		}
		if first {
			negated = neg
			if negated {
				// A negated list starts from "everything allowed, auth not required" and removes
				// methods; "!none" below flips authRequired back on.
				ra.allowed = 1<<authMethodCount - 1
			} else {
				ra.authRequired = true
			}
			first = false
		} else if neg != negated {
			if neg {
				return requireAuth{}, fmt.Errorf("negative require_auth method %q cannot be mixed with non-negative methods", method)
			}
			return requireAuth{}, fmt.Errorf("require_auth method %q cannot be mixed with negative methods", method)
		}

		var m authMethod
		switch method {
		case "password":
			m = authMethodPassword
		case "md5":
			m = authMethodMD5
		case "gss":
			m = authMethodGSS
		case "sspi":
			m = authMethodSSPI
		case "scram-sha-256":
			m = authMethodSCRAMSHA256
		case "oauth":
			m = authMethodOAuth
		case "none":
			m = authMethodNone
		default:
			return requireAuth{}, fmt.Errorf("invalid require_auth method: %q", method)
		}

		bit := uint8(1) << m
		if negated {
			if ra.allowed&bit == 0 {
				return requireAuth{}, fmt.Errorf("require_auth method %q is specified more than once", part)
			}
			ra.allowed &^= bit
			if m == authMethodNone {
				ra.authRequired = true
			}
		} else {
			if ra.allowed&bit != 0 {
				return requireAuth{}, fmt.Errorf("require_auth method %q is specified more than once", part)
			}
			ra.allowed |= bit
			if m == authMethodNone {
				ra.authRequired = false
			}
		}
	}

	return ra, nil
}
