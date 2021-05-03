package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
)

func main() {
	// urlExample := "postgres://immudb:immudb@localhost:5439/defaultdb"
	config, _ := pgx.ParseConfig("host=localhost port=5439 sslmode=disable user=immudb dbname=defaultdb password=immudb")
	config.PreferSimpleProtocol = true
	conn, err := pgx.ConnectConfig(context.Background(), config)
	//conn, err := pgx.Connect(context.Background(), tested)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "CREATE TABLE myTable2 (id INTEGER, amount INTEGER, title VARCHAR, PRIMARY KEY id)")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to exec into database: %v\n", err)
		os.Exit(1)
	}

	_, err = conn.Exec(context.Background(), "UPSERT INTO myTable2 (id, amount, title) VALUES (1, 200, 'title 1')")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to insert into database: %v\n", err)
		os.Exit(1)
	}
	_, err = conn.Exec(context.Background(), "UPSERT INTO myTable2(id, amount, title) VALUES (2, 400, 'title 2')")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to insert into database: %v\n", err)
		os.Exit(1)
	}

	var title string
	var id int64
	var amount int64
	err = conn.QueryRow(context.Background(), "SELECT id, amount, title FROM myTable2").Scan(&id, &amount, &title)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(id, amount, title)

	rows, err := conn.Query(context.Background(), "SELECT id, amount, title FROM myTable2")
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	for rows.Next() {
		rows.Scan(&id, &amount, &title)
		if err != nil {
			fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(id, amount, title)
	}
	if err := conn.Close(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Close conn failed: %v\n", err)
		os.Exit(1)
	}

}
