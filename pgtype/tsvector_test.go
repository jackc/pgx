package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEqTSVector(a any) func(any) bool {
	return func(v any) bool {
		at := a.(pgtype.TSVector)
		vt := v.(pgtype.TSVector)

		if len(at.Lexemes) != len(vt.Lexemes) {
			return false
		}

		if at.Valid != vt.Valid {
			return false
		}

		for i := range at.Lexemes {
			atLexeme := at.Lexemes[i]
			vtLexeme := vt.Lexemes[i]

			if atLexeme.Word != vtLexeme.Word {
				return false
			}

			if len(atLexeme.Positions) != len(vtLexeme.Positions) {
				return false
			}

			for j := range atLexeme.Positions {
				if atLexeme.Positions[j] != vtLexeme.Positions[j] {
					return false
				}
			}
		}

		return true
	}
}

func tsvectorConnTestRunner(t *testing.T) pgxtest.ConnTestRunner {
	ctr := defaultConnTestRunner
	ctr.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var tsvectorOID uint32
		err := conn.QueryRow(context.Background(), `select oid from pg_type where typname = 'tsvector'`).Scan(&tsvectorOID)
		if err != nil {
			t.Skipf("Skipping; cannot find tsvector OID")
		}

		conn.TypeMap().RegisterType(&pgtype.Type{Name: "tsvector", OID: tsvectorOID, Codec: pgtype.TSVectorCodec{}})
	}
	return ctr
}

func TestTSVectorCodecBinary(t *testing.T) {
	t.Run("Core", func(t *testing.T) {
		tests := []pgxtest.ValueRoundTripTest{
			// NULL.
			{
				Param:  pgtype.TSVector{},
				Result: new(pgtype.TSVector),
				Test:   isExpectedEqTSVector(pgtype.TSVector{}),
			},
			// Empty but valid tsvector (no lexemes).
			{
				Param:  pgtype.TSVector{Valid: true},
				Result: new(pgtype.TSVector),
				Test:   isExpectedEqTSVector(pgtype.TSVector{Valid: true}),
			},
			// Single lexeme with no positions.
			{
				Param: pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{{"fat", nil}},
					Valid:   true,
				},
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{{"fat", nil}},
					Valid:   true,
				}),
			},
			// Multiple lexemes with positions and weights.
			{
				Param: pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"cat", []pgtype.TSVectorPosition{{1, pgtype.TSVectorWeightA}}},
						{"dog", []pgtype.TSVectorPosition{{2, pgtype.TSVectorWeightB}}},
					},
					Valid: true,
				},
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"cat", []pgtype.TSVectorPosition{{1, pgtype.TSVectorWeightA}}},
						{"dog", []pgtype.TSVectorPosition{{2, pgtype.TSVectorWeightB}}},
					},
					Valid: true,
				}),
			},
			// All four weight types (A, B, C, D) on a single lexeme.
			{
				Param: pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"word", []pgtype.TSVectorPosition{
							{1, pgtype.TSVectorWeightA},
							{2, pgtype.TSVectorWeightB},
							{3, pgtype.TSVectorWeightC},
							{4, pgtype.TSVectorWeightD},
						}},
					},
					Valid: true,
				},
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"word", []pgtype.TSVectorPosition{
							{1, pgtype.TSVectorWeightA},
							{2, pgtype.TSVectorWeightB},
							{3, pgtype.TSVectorWeightC},
							{4, pgtype.TSVectorWeightD},
						}},
					},
					Valid: true,
				}),
			},
			// Multiple positions per lexeme.
			{
				Param: pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"spaceship", []pgtype.TSVectorPosition{
							{2, pgtype.TSVectorWeightD},
							{33, pgtype.TSVectorWeightA},
							{34, pgtype.TSVectorWeightB},
							{35, pgtype.TSVectorWeightC},
							{36, pgtype.TSVectorWeightD},
						}},
					},
					Valid: true,
				},
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"spaceship", []pgtype.TSVectorPosition{
							{2, pgtype.TSVectorWeightD},
							{33, pgtype.TSVectorWeightA},
							{34, pgtype.TSVectorWeightB},
							{35, pgtype.TSVectorWeightC},
							{36, pgtype.TSVectorWeightD},
						}},
					},
					Valid: true,
				}),
			},
			// Lexeme word containing a space.
			{
				Param: pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"1 2", nil},
					},
					Valid: true,
				},
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"1 2", nil},
					},
					Valid: true,
				}),
			},
		}

		pgxtest.RunValueRoundTripTests(context.Background(), t, tsvectorConnTestRunner(t), pgxtest.KnownOIDQueryExecModes, "tsvector", tests)
	})

	t.Run("SpecialCharacters", func(t *testing.T) {
		tests := []pgxtest.ValueRoundTripTest{
			// Lexeme words containing a single quote.
			{
				Param: pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"D'Artagnan", []pgtype.TSVectorPosition{}},
						{"cats'", []pgtype.TSVectorPosition{}},
						{"don't", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				},
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"D'Artagnan", []pgtype.TSVectorPosition{}},
						{"cats'", []pgtype.TSVectorPosition{}},
						{"don't", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				}),
			},
			// Unicode lexemes.
			{
				Param: pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"café", []pgtype.TSVectorPosition{}},
						{"naïve", []pgtype.TSVectorPosition{}},
						{"日本語", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				},
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"café", []pgtype.TSVectorPosition{}},
						{"naïve", []pgtype.TSVectorPosition{}},
						{"日本語", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				}),
			},
			// Lexeme words containing backslashes.
			{
				Param: pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{`ab\c`, []pgtype.TSVectorPosition{}},
						{`back\slash`, []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				},
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{`ab\c`, []pgtype.TSVectorPosition{}},
						{`back\slash`, []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				}),
			},
			// Lexeme words containing delimiter characters (colon, comma).
			{
				Param: pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"a:b", []pgtype.TSVectorPosition{}},
						{"c,d", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				},
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"a:b", []pgtype.TSVectorPosition{}},
						{"c,d", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				}),
			},
		}

		pgxtest.RunValueRoundTripTests(context.Background(), t, tsvectorConnTestRunner(t), pgxtest.KnownOIDQueryExecModes, "tsvector", tests)
	})
}

func TestTSVectorCodecText(t *testing.T) {
	t.Run("Core", func(t *testing.T) {
		tests := []pgxtest.ValueRoundTripTest{
			// NULL.
			{
				Param:  pgtype.TSVector{},
				Result: new(pgtype.TSVector),
				Test:   isExpectedEqTSVector(pgtype.TSVector{}),
			},
			// Empty but valid tsvector (no lexemes).
			{
				Param:  pgtype.TSVector{Valid: true},
				Result: new(pgtype.TSVector),
				Test:   isExpectedEqTSVector(pgtype.TSVector{Valid: true}),
			},
			// Single lexeme with no positions.
			{
				Param:  "'fat'",
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{{"fat", nil}},
					Valid:   true,
				}),
			},
			// Multiple lexemes with positions and weights.
			{
				Param:  "'cat':1A 'dog':2B",
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"cat", []pgtype.TSVectorPosition{{1, pgtype.TSVectorWeightA}}},
						{"dog", []pgtype.TSVectorPosition{{2, pgtype.TSVectorWeightB}}},
					},
					Valid: true,
				}),
			},
			// All four weight types (A, B, C, D) on a single lexeme.
			{
				Param:  "'word':1A,2B,3C,4D",
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"word", []pgtype.TSVectorPosition{
							{1, pgtype.TSVectorWeightA},
							{2, pgtype.TSVectorWeightB},
							{3, pgtype.TSVectorWeightC},
							{4, pgtype.TSVectorWeightD},
						}},
					},
					Valid: true,
				}),
			},
			// Multiple positions per lexeme.
			{
				Param:  "'spaceship':2,33A,34B,35C,36D",
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"spaceship", []pgtype.TSVectorPosition{
							{2, pgtype.TSVectorWeightD},
							{33, pgtype.TSVectorWeightA},
							{34, pgtype.TSVectorWeightB},
							{35, pgtype.TSVectorWeightC},
							{36, pgtype.TSVectorWeightD},
						}},
					},
					Valid: true,
				}),
			},
			// Lowercase weight letters are accepted and normalized to uppercase.
			{
				Param:  "'cat':2b",
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"cat", []pgtype.TSVectorPosition{{2, pgtype.TSVectorWeightB}}},
					},
					Valid: true,
				}),
			},
			// Leading and trailing whitespace is trimmed.
			{
				Param:  "  'fat'  ",
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"fat", nil},
					},
					Valid: true,
				}),
			},
			// Lexeme word containing a space.
			{
				Param:  "'1 2'",
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"1 2", nil},
					},
					Valid: true,
				}),
			},
			// Backslash quote escape (\').
			{
				Param:  `'D\'Artagnan' 'cats\'' 'don\'t'`,
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"D'Artagnan", []pgtype.TSVectorPosition{}},
						{"cats'", []pgtype.TSVectorPosition{}},
						{"don't", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				}),
			},
			// Lexeme words containing delimiter characters (colon, comma).
			{
				Param:  `'a:b' 'c,d'`,
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"a:b", []pgtype.TSVectorPosition{}},
						{"c,d", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				}),
			},
		}

		pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.AllQueryExecModes, "tsvector", tests)
	})

	t.Run("SpecialCharacters", func(t *testing.T) {
		tests := []pgxtest.ValueRoundTripTest{
			// Unicode lexemes.
			{
				Param:  "'café' 'naïve' '日本語'",
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"café", []pgtype.TSVectorPosition{}},
						{"naïve", []pgtype.TSVectorPosition{}},
						{"日本語", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				}),
			},
			// Escaped space in lexeme word.
			{
				Param:  `'\ '`,
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{" ", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				}),
			},
		}

		pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.AllQueryExecModes, "tsvector", tests)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		skipCockroachDB(t, "CockroachDB does not support these escape sequences in tsvector")

		tests := []pgxtest.ValueRoundTripTest{
			// Doubled quote escape ('').
			{
				Param:  `'D''Artagnan' 'cats''' 'don''t'`,
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"D'Artagnan", []pgtype.TSVectorPosition{}},
						{"cats'", []pgtype.TSVectorPosition{}},
						{"don't", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				}),
			},
			// Escaped backslashes in lexeme words.
			{
				Param:  `'AB\\\c' '\\as' 'ab\\\\c' 'ab\\c' 'abc'`,
				Result: new(pgtype.TSVector),
				Test: isExpectedEqTSVector(pgtype.TSVector{
					Lexemes: []pgtype.TSVectorLexeme{
						{"AB\\c", []pgtype.TSVectorPosition{}},
						{"\\as", []pgtype.TSVectorPosition{}},
						{"ab\\\\c", []pgtype.TSVectorPosition{}},
						{"ab\\c", []pgtype.TSVectorPosition{}},
						{"abc", []pgtype.TSVectorPosition{}},
					},
					Valid: true,
				}),
			},
		}

		pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.AllQueryExecModes, "tsvector", tests)
	})
}
