package main

import (
	"bufio"
	"fmt"
	"github.com/jackc/pgx"
	"os"
	"time"
)

var pool *pgx.ConnPool

func main() {
	var err error
	pool, err = pgx.NewConnPool(extractConfig())
	if err != nil {
		fmt.Fprintln(os.Stderr, "Unable to connect to database:", err)
		os.Exit(1)
	}

	go listen()

	fmt.Println(`Type a message and press enter.

This message should appear in any other chat instances connected to the same
database.

Type "exit" to quit.
`)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		msg := scanner.Text()
		if msg == "exit" {
			os.Exit(0)
		}

		_, err = pool.Exec("select pg_notify('chat', $1)", msg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error sending notification:", err)
			os.Exit(1)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error scanning from stdin:", err)
		os.Exit(1)
	}
}

func listen() {
	conn, err := pool.Acquire()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error acquiring connection:", err)
		os.Exit(1)
	}
	defer pool.Release(conn)

	conn.Listen("chat")

	for {
		notification, err := conn.WaitForNotification(time.Second)
		if err == pgx.ErrNotificationTimeout {
			continue
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error waiting for notification:", err)
			os.Exit(1)
		}

		fmt.Println("PID:", notification.Pid, "Channel:", notification.Channel, "Payload:", notification.Payload)
	}
}

func extractConfig() pgx.ConnPoolConfig {
	var config pgx.ConnPoolConfig

	config.Host = os.Getenv("CHAT_DB_HOST")
	if config.Host == "" {
		config.Host = "localhost"
	}

	config.User = os.Getenv("CHAT_DB_USER")
	if config.User == "" {
		config.User = os.Getenv("USER")
	}

	config.Password = os.Getenv("CHAT_DB_PASSWORD")

	config.Database = os.Getenv("CHAT_DB_DATABASE")
	if config.Database == "" {
		config.Database = "postgres"
	}

	return config
}
