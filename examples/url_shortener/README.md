# Description

This is a sample REST URL shortener service implemented using pgx as the connector to a PostgreSQL data store.

# Usage

Create a PostgreSQL database and run structure.sql into it to create the necessary data schema.

Configure the database connection with `DATABASE_URL` or standard PostgreSQL (`PG*`) environment variables or

Run main.go:

```
go run main.go
```

## Create or Update a Shortened URL

```
curl -X PUT -d 'http://www.google.com' http://localhost:8080/google
```

## Get a Shortened URL

```
curl http://localhost:8080/google
```

## Delete a Shortened URL

```
curl -X DELETE http://localhost:8080/google
```
