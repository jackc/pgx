package main

import (
	"context"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/log15adapter"
	"github.com/jackc/pgx/v4/pgxpool"
	log "gopkg.in/inconshreveable/log15.v2"
)

var db *pgxpool.Pool

// afterConnect creates the prepared statements that this application uses
func afterConnect(ctx context.Context, conn *pgx.Conn) (err error) {
	_, err = conn.Prepare(ctx, "getUrl", `
    select url from shortened_urls where id=$1
  `)
	if err != nil {
		return
	}

	_, err = conn.Prepare(ctx, "deleteUrl", `
    delete from shortened_urls where id=$1
  `)
	if err != nil {
		return
	}

	_, err = conn.Prepare(ctx, "putUrl", `
    insert into shortened_urls(id, url) values ($1, $2)
    on conflict (id) do update set url=excluded.url
  `)
	return
}

func getUrlHandler(w http.ResponseWriter, req *http.Request) {
	var url string
	err := db.QueryRow(context.Background(), "getUrl", req.URL.Path).Scan(&url)
	switch err {
	case nil:
		http.Redirect(w, req, url, http.StatusSeeOther)
	case pgx.ErrNoRows:
		http.NotFound(w, req)
	default:
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

	if _, err := db.Exec(context.Background(), "putUrl", id, url); err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func deleteUrlHandler(w http.ResponseWriter, req *http.Request) {
	if _, err := db.Exec(context.Background(), "deleteUrl", req.URL.Path); err == nil {
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
	logger := log15adapter.NewLogger(log.New("module", "pgx"))

	poolConfig, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Crit("Unable to parse DATABASE_URL", "error", err)
		os.Exit(1)
	}

	poolConfig.AfterConnect = afterConnect
	poolConfig.ConnConfig.Logger = logger

	db, err = pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		log.Crit("Unable to create connection pool", "error", err)
		os.Exit(1)
	}

	http.HandleFunc("/", urlHandler)

	log.Info("Starting URL shortener on localhost:8080")
	err = http.ListenAndServe("localhost:8080", nil)
	if err != nil {
		log.Crit("Unable to start web server", "error", err)
		os.Exit(1)
	}
}
