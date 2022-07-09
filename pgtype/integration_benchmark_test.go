package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int16_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int16
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int16_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int16
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int16_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int16
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int16_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int16
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int16_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int16
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int16_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int16
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int16_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int16
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int16_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int16
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int32_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int32_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int32_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int32_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int32_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int32_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int32_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int32_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int64_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int64_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int64_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int64_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int64_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int64_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_int64_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_int64_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_uint64_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]uint64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_uint64_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]uint64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_uint64_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]uint64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_uint64_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]uint64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_uint64_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]uint64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_uint64_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]uint64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_uint64_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]uint64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_uint64_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]uint64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_pgtype_Int4_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]pgtype.Int4
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_pgtype_Int4_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]pgtype.Int4
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_pgtype_Int4_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]pgtype.Int4
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_pgtype_Int4_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]pgtype.Int4
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_pgtype_Int4_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]pgtype.Int4
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_pgtype_Int4_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]pgtype.Int4
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_int4_to_Go_pgtype_Int4_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]pgtype.Int4
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_int4_to_Go_pgtype_Int4_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]pgtype.Int4
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::int4 + 0, n::int4 + 1, n::int4 + 2, n::int4 + 3, n::int4 + 4, n::int4 + 5, n::int4 + 6, n::int4 + 7, n::int4 + 8, n::int4 + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_int64_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_int64_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_int64_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_int64_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_int64_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_int64_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_int64_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_int64_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]int64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_float64_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]float64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_float64_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]float64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_float64_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]float64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_float64_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]float64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_float64_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]float64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_float64_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]float64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_float64_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]float64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_float64_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]float64
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_pgtype_Numeric_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]pgtype.Numeric
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_pgtype_Numeric_1_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]pgtype.Numeric
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_pgtype_Numeric_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]pgtype.Numeric
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_pgtype_Numeric_1_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]pgtype.Numeric
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 1) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_pgtype_Numeric_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]pgtype.Numeric
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_pgtype_Numeric_10_rows_1_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [1]pgtype.Numeric
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0 from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_numeric_to_Go_pgtype_Numeric_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]pgtype.Numeric
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_numeric_to_Go_pgtype_Numeric_100_rows_10_columns(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v [10]pgtype.Numeric
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select n::numeric + 0, n::numeric + 1, n::numeric + 2, n::numeric + 3, n::numeric + 4, n::numeric + 5, n::numeric + 6, n::numeric + 7, n::numeric + 8, n::numeric + 9 from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_Int4Array_With_Go_Int4Array_10(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v []int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select array_agg(n) from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_Int4Array_With_Go_Int4Array_10(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v []int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select array_agg(n) from generate_series(1, 10) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_Int4Array_With_Go_Int4Array_100(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v []int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select array_agg(n) from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_Int4Array_With_Go_Int4Array_100(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v []int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select array_agg(n) from generate_series(1, 100) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryTextFormatDecode_PG_Int4Array_With_Go_Int4Array_1000(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v []int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select array_agg(n) from generate_series(1, 1000) n`,
				[]any{pgx.QueryResultFormats{pgx.TextFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkQueryBinaryFormatDecode_PG_Int4Array_With_Go_Int4Array_1000(b *testing.B) {
	defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		b.ResetTimer()
		var v []int32
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(
				ctx,
				`select array_agg(n) from generate_series(1, 1000) n`,
				[]any{pgx.QueryResultFormats{pgx.BinaryFormatCode}},
			)
			_, err := pgx.ForEachRow(rows, []any{&v}, func() error { return nil })
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
