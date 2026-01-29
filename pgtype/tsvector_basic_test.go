package pgtype_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestTSVectorBasic(t *testing.T) {
	ctx := context.Background()
	
	connStr := os.Getenv("PGX_TEST_DATABASE")
	if connStr == "" {
		t.Skip("PGX_TEST_DATABASE not set")
	}
	
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close(ctx)

	t.Run("scan from to_tsvector", func(t *testing.T) {
		var tsv pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT to_tsvector('english', 'The quick brown fox')").Scan(&tsv)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if !tsv.Valid {
			t.Error("Expected valid tsvector")
		}

		if len(tsv.Lexemes) == 0 {
			t.Error("Expected lexemes")
		}

		t.Logf("Found %d lexemes", len(tsv.Lexemes))
		for _, lex := range tsv.Lexemes {
			t.Logf("  - %s (positions: %d)", lex.Word, len(lex.Positions))
		}
	})

	t.Run("encode and decode", func(t *testing.T) {
		original := pgtype.TSVector{
			Lexemes: []pgtype.Lexeme{
				{
					Word: "cat",
					Positions: []pgtype.LexemePosition{
						{Position: 1, Weight: pgtype.WeightA},
						{Position: 5, Weight: pgtype.WeightB},
					},
				},
				{
					Word: "dog",
					Positions: []pgtype.LexemePosition{
						{Position: 2},
					},
				},
			},
			Valid: true,
		}

		var result pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT $1::tsvector", original).Scan(&result)
		if err != nil {
			t.Fatalf("Round-trip failed: %v", err)
		}

		if !result.Valid {
			t.Error("Expected valid result")
		}

		if len(result.Lexemes) != 2 {
			t.Errorf("Expected 2 lexemes, got %d", len(result.Lexemes))
		}
	})

	t.Run("null tsvector", func(t *testing.T) {
		var tsv pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT NULL::tsvector").Scan(&tsv)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if tsv.Valid {
			t.Error("Expected invalid tsvector for NULL")
		}
	})
}
