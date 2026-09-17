package pgx

import "testing"

func TestTxOptionsBeginSQL(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		txOptions TxOptions
		want      string
	}{
		{"default", TxOptions{}, "begin"},
		{"commit query only", TxOptions{CommitQuery: "commit and chain"}, "begin"},
		{"isolation level", TxOptions{IsoLevel: ReadCommitted}, "begin isolation level read committed"},
		{"access mode", TxOptions{AccessMode: ReadOnly}, "begin read only"},
		{"deferrable mode", TxOptions{DeferrableMode: Deferrable}, "begin deferrable"},
		{
			"every option",
			TxOptions{IsoLevel: Serializable, AccessMode: ReadOnly, DeferrableMode: NotDeferrable},
			"begin isolation level serializable read only not deferrable",
		},
		{"begin query overrides the options", TxOptions{IsoLevel: Serializable, BeginQuery: "begin priority high"}, "begin priority high"},
		{"custom isolation level", TxOptions{IsoLevel: "snapshot", AccessMode: ReadWrite}, "begin isolation level snapshot read write"},
		{"custom access mode", TxOptions{AccessMode: "read write only"}, "begin read write only"},
		{"custom deferrable mode", TxOptions{DeferrableMode: "maybe deferrable"}, "begin maybe deferrable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.txOptions.beginSQL(); got != tt.want {
				t.Errorf("beginSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTxOptionsBeginSQLEveryStandardCombination(t *testing.T) {
	t.Parallel()

	for _, isoLevel := range []TxIsoLevel{"", Serializable, RepeatableRead, ReadCommitted, ReadUncommitted} {
		for _, accessMode := range []TxAccessMode{"", ReadWrite, ReadOnly} {
			for _, deferrableMode := range []TxDeferrableMode{"", Deferrable, NotDeferrable} {
				txOptions := TxOptions{IsoLevel: isoLevel, AccessMode: accessMode, DeferrableMode: deferrableMode}
				want := renderBeginSQL(isoLevel, accessMode, deferrableMode)
				if got := txOptions.beginSQL(); got != want {
					t.Errorf("%+v: beginSQL() = %q, want %q", txOptions, got, want)
				}
			}
		}
	}
}

func BenchmarkTxOptionsBeginSQL(b *testing.B) {
	for _, bb := range []struct {
		name      string
		txOptions TxOptions
	}{
		{"default", TxOptions{}},
		{"isolation level", TxOptions{IsoLevel: ReadCommitted}},
		{"every option", TxOptions{IsoLevel: Serializable, AccessMode: ReadOnly, DeferrableMode: NotDeferrable}},
		{"custom isolation level", TxOptions{IsoLevel: "snapshot"}},
	} {
		b.Run(bb.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				bb.txOptions.beginSQL()
			}
		})
	}
}
