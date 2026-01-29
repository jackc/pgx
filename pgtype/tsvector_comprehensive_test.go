package pgtype_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestTSVectorComprehensive(t *testing.T) {
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

	t.Run("empty tsvector", func(t *testing.T) {
		var tsv pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT ''::tsvector").Scan(&tsv)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if !tsv.Valid {
			t.Error("Expected valid tsvector")
		}
		if len(tsv.Lexemes) != 0 {
			t.Errorf("Expected 0 lexemes, got %d", len(tsv.Lexemes))
		}
	})

	t.Run("multiple lexemes with positions", func(t *testing.T) {
		var tsv pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT 'cat:1,5 dog:2 bird:3,4,6'::tsvector").Scan(&tsv)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(tsv.Lexemes) != 3 {
			t.Errorf("Expected 3 lexemes, got %d", len(tsv.Lexemes))
		}

		// Lexemes should be sorted
		expectedWords := []string{"bird", "cat", "dog"}
		for i, expected := range expectedWords {
			if i >= len(tsv.Lexemes) {
				t.Errorf("Missing lexeme at index %d", i)
				continue
			}
			if tsv.Lexemes[i].Word != expected {
				t.Errorf("Lexeme %d: expected %q, got %q", i, expected, tsv.Lexemes[i].Word)
			}
		}
	})

	t.Run("weights", func(t *testing.T) {
		var tsv pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT 'cat:1A,2B,3C,4D'::tsvector").Scan(&tsv)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(tsv.Lexemes) != 1 {
			t.Fatalf("Expected 1 lexeme, got %d", len(tsv.Lexemes))
		}

		if len(tsv.Lexemes[0].Positions) != 4 {
			t.Fatalf("Expected 4 positions, got %d", len(tsv.Lexemes[0].Positions))
		}

		expectedWeights := []pgtype.Weight{
			pgtype.WeightA,
			pgtype.WeightB,
			pgtype.WeightC,
			pgtype.WeightD,
		}

		for i, expected := range expectedWeights {
			if tsv.Lexemes[0].Positions[i].Weight != expected {
				t.Errorf("Position %d: expected weight %v, got %v", 
					i, expected, tsv.Lexemes[0].Positions[i].Weight)
			}
		}
	})

	t.Run("quoted strings with spaces", func(t *testing.T) {
		// Note: In PostgreSQL tsvector syntax, you need to use '''hello world'':1' 
		// (with triple quotes) to keep the phrase together
		var tsv pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT '''hello world'':1'::tsvector").Scan(&tsv)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(tsv.Lexemes) != 1 {
			t.Fatalf("Expected 1 lexeme, got %d", len(tsv.Lexemes))
		}

		if tsv.Lexemes[0].Word != "hello world" {
			t.Errorf("Expected 'hello world', got %q", tsv.Lexemes[0].Word)
		}
	})

	t.Run("encode custom tsvector with spaces", func(t *testing.T) {
		// Test that we can encode a lexeme with spaces
		original := pgtype.TSVector{
			Lexemes: []pgtype.Lexeme{
				{
					Word: "hello world",
					Positions: []pgtype.LexemePosition{
						{Position: 1},
					},
				},
			},
			Valid: true,
		}

		var result pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT $1::tsvector", original).Scan(&result)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		if len(result.Lexemes) != 1 {
			t.Fatalf("Expected 1 lexeme, got %d", len(result.Lexemes))
		}

		if result.Lexemes[0].Word != "hello world" {
			t.Errorf("Expected 'hello world', got %q", result.Lexemes[0].Word)
		}
	})

	t.Run("encode custom tsvector", func(t *testing.T) {
		original := pgtype.TSVector{
			Lexemes: []pgtype.Lexeme{
				{
					Word: "postgresql",
					Positions: []pgtype.LexemePosition{
						{Position: 1, Weight: pgtype.WeightA},
					},
				},
				{
					Word: "database",
					Positions: []pgtype.LexemePosition{
						{Position: 2, Weight: pgtype.WeightB},
					},
				},
			},
			Valid: true,
		}

		var result string
		err := conn.QueryRow(ctx, "SELECT $1::tsvector::text", original).Scan(&result)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		t.Logf("Encoded as: %s", result)

		// Now decode it back
		var decoded pgtype.TSVector
		err = conn.QueryRow(ctx, "SELECT $1::tsvector", original).Scan(&decoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if len(decoded.Lexemes) != 2 {
			t.Errorf("Expected 2 lexemes after round-trip, got %d", len(decoded.Lexemes))
		}
	})

	t.Run("real world - full text search", func(t *testing.T) {
		// Create a temp table
		_, err := conn.Exec(ctx, `
			CREATE TEMP TABLE documents (
				id SERIAL PRIMARY KEY,
				title TEXT,
				content TEXT,
				search_vector TSVECTOR
			)
		`)
		if err != nil {
			t.Fatalf("Create table failed: %v", err)
		}

		// Insert documents
		_, err = conn.Exec(ctx, `
			INSERT INTO documents (title, content, search_vector) VALUES
			('PostgreSQL Tutorial', 'Learn PostgreSQL database management', 
			 to_tsvector('english', 'PostgreSQL Tutorial Learn PostgreSQL database management')),
			('Go Programming', 'Introduction to Go programming language',
			 to_tsvector('english', 'Go Programming Introduction to Go programming language'))
		`)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		// Query and scan tsvector
		var title string
		var tsv pgtype.TSVector
		err = conn.QueryRow(ctx, 
			"SELECT title, search_vector FROM documents WHERE title = $1",
			"PostgreSQL Tutorial").Scan(&title, &tsv)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if !tsv.Valid {
			t.Error("Expected valid tsvector")
		}

		if len(tsv.Lexemes) == 0 {
			t.Error("Expected lexemes in search vector")
		}

		t.Logf("Document '%s' has %d lexemes", title, len(tsv.Lexemes))

		// Check for expected words
		foundPostgres := false
		for _, lex := range tsv.Lexemes {
			if lex.Word == "postgresql" || lex.Word == "postgr" {
				foundPostgres = true
				break
			}
		}

		if !foundPostgres {
			t.Error("Expected to find 'postgresql' or 'postgr' in lexemes")
		}
	})

	t.Run("normalization - sorting", func(t *testing.T) {
		// Unsorted input
		original := pgtype.TSVector{
			Lexemes: []pgtype.Lexeme{
				{Word: "zebra"},
				{Word: "apple"},
				{Word: "mango"},
			},
			Valid: true,
		}

		var result pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT $1::tsvector", original).Scan(&result)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Should be sorted alphabetically
		if len(result.Lexemes) != 3 {
			t.Fatalf("Expected 3 lexemes, got %d", len(result.Lexemes))
		}

		if result.Lexemes[0].Word != "apple" {
			t.Errorf("Expected first lexeme to be 'apple', got %q", result.Lexemes[0].Word)
		}
		if result.Lexemes[1].Word != "mango" {
			t.Errorf("Expected second lexeme to be 'mango', got %q", result.Lexemes[1].Word)
		}
		if result.Lexemes[2].Word != "zebra" {
			t.Errorf("Expected third lexeme to be 'zebra', got %q", result.Lexemes[2].Word)
		}
	})

	t.Run("normalization - position deduplication", func(t *testing.T) {
		original := pgtype.TSVector{
			Lexemes: []pgtype.Lexeme{
				{
					Word: "test",
					Positions: []pgtype.LexemePosition{
						{Position: 5},
						{Position: 3},
						{Position: 5}, // duplicate
						{Position: 1},
					},
				},
			},
			Valid: true,
		}

		var result pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT $1::tsvector", original).Scan(&result)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Lexemes[0].Positions) != 3 {
			t.Errorf("Expected 3 positions after deduplication, got %d", 
				len(result.Lexemes[0].Positions))
		}

		// Should be sorted
		if result.Lexemes[0].Positions[0].Position != 1 {
			t.Errorf("Expected first position to be 1, got %d", 
				result.Lexemes[0].Positions[0].Position)
		}
	})

	t.Run("unicode support", func(t *testing.T) {
		original := pgtype.TSVector{
			Lexemes: []pgtype.Lexeme{
				{Word: "日本語", Positions: []pgtype.LexemePosition{{Position: 1}}},
				{Word: "français", Positions: []pgtype.LexemePosition{{Position: 2}}},
			},
			Valid: true,
		}

		var result pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT $1::tsvector", original).Scan(&result)
		if err != nil {
			t.Fatalf("Unicode test failed: %v", err)
		}

		if !result.Valid {
			t.Error("Expected valid result")
		}
	})

	t.Run("max position value", func(t *testing.T) {
		original := pgtype.TSVector{
			Lexemes: []pgtype.Lexeme{
				{
					Word: "test",
					Positions: []pgtype.LexemePosition{
						{Position: 16383}, // Maximum allowed
					},
				},
			},
			Valid: true,
		}

		var result pgtype.TSVector
		err := conn.QueryRow(ctx, "SELECT $1::tsvector", original).Scan(&result)
		if err != nil {
			t.Fatalf("Max position test failed: %v", err)
		}

		if result.Lexemes[0].Positions[0].Position != 16383 {
			t.Errorf("Expected position 16383, got %d", 
				result.Lexemes[0].Positions[0].Position)
		}
	})
}
