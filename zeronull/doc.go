// Package zeronull contains types that automatically convert between database NULLs and Go zero values.
/*
Sometimes the distinction between a zero value and a NULL value is not useful at the application level. For example,
in PostgreSQL an empty string may be stored as NULL. There is usually no application level distinction between an
empty string and a NULL string. Package zeronull implements types that seamlessly convert between PostgreSQL NULL and
the zero value.

It is recommended to convert types at usage time rather than instantiate these types directly. In the example below,
middlename would be stored as a NULL.

		firstname := "John"
		middlename := ""
		lastname := "Smith"
		_, err := conn.Exec(
			ctx,
			"insert into people(firstname, middlename, lastname) values($1, $2, $3)",
			zeronull.Text(firstname),
			zeronull.Text(middlename),
			zeronull.Text(lastname),
		)
*/
package zeronull
