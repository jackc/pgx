package main

import (
	"fmt"
	"github.com/jackc/pgx"
	"io/ioutil"
	"net/http"
	"os"
)

var pool *pgx.ConnectionPool

// afterConnect creates the prepared statements that this application uses
func afterConnect(conn *pgx.Connection) (err error) {
	err = conn.Prepare("getUrl", `
    select url from shortened_urls where id=$1
  `)
	if err != nil {
		return
	}

	err = conn.Prepare("deleteUrl", `
    delete from shortened_urls where id=$1
  `)
	if err != nil {
		return
	}

	// There technically is a small race condition in doing an upsert with a CTE
	// where one of two simultaneous requests to the shortened URL would fail
	// with a unique index violation. As the point of this demo is pgx usage and
	// not how to perfectly upsert in PostgreSQL it is deemed acceptable.
	err = conn.Prepare("putUrl", `
    with upsert as (
      update shortened_urls
      set url=$2
      where id=$1
      returning *
    )
    insert into shortened_urls(id, url)
    select $1, $2 where not exists(select 1 from upsert)
  `)
	return
}

func getUrlHandler(w http.ResponseWriter, req *http.Request) {
	if url, err := pool.SelectValue("getUrl", req.URL.Path); err == nil {
		http.Redirect(w, req, url.(string), http.StatusSeeOther)
	} else if _, ok := err.(pgx.NotSingleRowError); ok {
		http.NotFound(w, req)
	} else {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func putUrlHandler(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Path
	var url string
	if body, err := ioutil.ReadAll(req.Body); err == nil {
		url = string(body)
	} else {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if _, err := pool.Execute("putUrl", id, url); err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func deleteUrlHandler(w http.ResponseWriter, req *http.Request) {
	if _, err := pool.Execute("deleteUrl", req.URL.Path); err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func urlHandler(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		getUrlHandler(w, req)

	case "PUT":
		putUrlHandler(w, req)

	case "DELETE":
		deleteUrlHandler(w, req)

	default:
		w.Header().Add("Allow", "GET, PUT, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	var err error
	connectionOptions := pgx.ConnectionParameters{
		Host:     "127.0.0.1",
		User:     "jack",
		Password: "jack",
		Database: "url_shortener"}
	poolOptions := pgx.ConnectionPoolOptions{MaxConnections: 5, AfterConnect: afterConnect}
	pool, err = pgx.NewConnectionPool(connectionOptions, poolOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}

	http.HandleFunc("/", urlHandler)

	fmt.Println("Starting URL shortener on localhost:8080...")
	err = http.ListenAndServe("localhost:8080", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to start web server: %v\n", err)
		os.Exit(1)
	}
}
