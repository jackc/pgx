package pgconn

import (
	"strings"
	"testing"
)

func TestParseRequireAuth(t *testing.T) {
	type want struct {
		authRequired bool
		allow        []authMethod
		forbid       []authMethod
	}
	for _, tc := range []struct {
		in   string
		want want
		err  string
	}{
		{in: "", want: want{authRequired: false, allow: []authMethod{authMethodPassword, authMethodMD5, authMethodGSS, authMethodSSPI, authMethodSCRAMSHA256, authMethodOAuth, authMethodNone}}},
		{in: "scram-sha-256", want: want{authRequired: true, allow: []authMethod{authMethodSCRAMSHA256}, forbid: []authMethod{authMethodPassword, authMethodMD5, authMethodNone}}},
		{in: "md5,scram-sha-256", want: want{authRequired: true, allow: []authMethod{authMethodMD5, authMethodSCRAMSHA256}, forbid: []authMethod{authMethodPassword, authMethodNone}}},
		{in: "none", want: want{authRequired: false, allow: []authMethod{authMethodNone}, forbid: []authMethod{authMethodPassword, authMethodSCRAMSHA256}}},
		{in: "scram-sha-256,none", want: want{authRequired: false, allow: []authMethod{authMethodSCRAMSHA256, authMethodNone}, forbid: []authMethod{authMethodPassword}}},
		{in: "!password", want: want{authRequired: false, allow: []authMethod{authMethodMD5, authMethodSCRAMSHA256, authMethodNone}, forbid: []authMethod{authMethodPassword}}},
		{in: "!password,!md5", want: want{authRequired: false, allow: []authMethod{authMethodSCRAMSHA256, authMethodGSS, authMethodNone}, forbid: []authMethod{authMethodPassword, authMethodMD5}}},
		{in: "!none", want: want{authRequired: true, allow: []authMethod{authMethodPassword, authMethodMD5, authMethodSCRAMSHA256}, forbid: []authMethod{authMethodNone}}},
		{in: " scram-sha-256 , md5 ", want: want{authRequired: true, allow: []authMethod{authMethodSCRAMSHA256, authMethodMD5}, forbid: []authMethod{authMethodPassword}}},

		{in: "bogus", err: "invalid require_auth method"},
		{in: "md5,md5", err: "specified more than once"},
		{in: "!md5,!md5", err: "specified more than once"},
		{in: "md5,!password", err: "cannot be mixed"},
		{in: "!md5,password", err: "cannot be mixed"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			ra, err := parseRequireAuth(tc.in)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("expected error containing %q, got %v", tc.err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ra.authRequired != tc.want.authRequired {
				t.Errorf("authRequired = %v, want %v", ra.authRequired, tc.want.authRequired)
			}
			for _, m := range tc.want.allow {
				if !ra.allows(m) {
					t.Errorf("expected %s allowed", authMethodNames[m])
				}
			}
			for _, m := range tc.want.forbid {
				if ra.allows(m) {
					t.Errorf("expected %s forbidden", authMethodNames[m])
				}
			}
		})
	}
}

func TestRequireAuthCheckMessages(t *testing.T) {
	ra, _ := parseRequireAuth("scram-sha-256")
	if err := ra.check(authMethodSCRAMSHA256); err != nil {
		t.Fatalf("scram-sha-256 should be allowed: %v", err)
	}
	if err := ra.check(authMethodPassword); err == nil || !strings.Contains(err.Error(), `requirement "scram-sha-256" failed: server requested password`) {
		t.Fatalf("wrong error for password: %v", err)
	}
	if err := ra.check(authMethodNone); err == nil || !strings.Contains(err.Error(), "server did not complete authentication") {
		t.Fatalf("wrong error for none: %v", err)
	}
}
