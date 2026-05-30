package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

var pool *pgxpool.Pool

func main() {
	var err error
	pool, err = pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Unable to connect to database:", err)
		os.Exit(1)
	}

	go func() {
		if err := listen(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}()

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

func listen() error {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(context.Background(), "listen chat")
	if err != nil {
		return fmt.Errorf("listening to chat channel: %w", err)
	}

	for {
		notification, err := conn.Conn().WaitForNotification(context.Background())
		if err != nil {
			return fmt.Errorf("waiting for notification: %w", err)
		}

		fmt.Println("PID:", notification.PID, "Channel:", notification.Channel, "Payload:", notification.Payload)
	}
}
