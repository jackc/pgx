package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
)

var pool *pgxpool.Pool

func main() {
	var err error
	pool, err = pgxpool.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Unable to connect to database:", err)
		os.Exit(1)
	}

	go listen()

	fmt.Println(`Type a message and press enter.

This message should appear in any other chat instances connected to the same
database.

Type "exit" to quit.`)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		msg := scanner.Text()
		if msg == "exit" {
			os.Exit(0)
		}

		_, err = pool.Exec(context.Background(), "select pg_notify('chat', $1)", msg)
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
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error acquiring connection:", err)
		os.Exit(1)
	}
	defer conn.Release()

	_, err = conn.Exec(context.Background(), "listen chat")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error listening to chat channel:", err)
		os.Exit(1)
	}

	for {
		notification, err := conn.Conn().WaitForNotification(context.Background())
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error waiting for notification:", err)
			os.Exit(1)
		}

		fmt.Println("PID:", notification.PID, "Channel:", notification.Channel, "Payload:", notification.Payload)
	}
}
