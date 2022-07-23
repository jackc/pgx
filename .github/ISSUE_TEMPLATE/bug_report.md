---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:

If possible, please provide runnable example such as:

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v4"
)

func main() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	// Your code here...
}
```

**Expected behavior**
A clear and concise description of what you expected to happen.

**Actual behavior**
A clear and concise description of what actually happened.

**Version**
 - Go: `$ go version` -> [e.g. go version go1.18.3 darwin/amd64]
 - PostgreSQL: `$ psql --no-psqlrc --tuples-only -c 'select version()'` -> [e.g. PostgreSQL 14.4 on x86_64-apple-darwin21.5.0, compiled by Apple clang version 13.1.6 (clang-1316.0.21.2.5), 64-bit]
 - pgx: `$ grep 'github.com/jackc/pgx/v[0-9]' go.mod` -> [e.g. v4.16.1]

**Additional context**
Add any other context about the problem here.
