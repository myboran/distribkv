// only for learning. from  https://github.com/YuriyNasretdinov/distribkv
package main

import (
	"distribkv/db"
	"distribkv/web"
	"flag"
	"log"
	"net/http"
)

var (
	dbLocation = flag.String("db-location", "", "the path to the bolt db database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
)

func parseFlags() {
	flag.Parse()

	if *dbLocation == "" {
		log.Fatal("must input db path")
	}
}

func main() {
	parseFlags()

	db, err := db.NewDatabase(*dbLocation)
	if err != nil {
		log.Fatalf("NewDatabase (%q) err: %v", *dbLocation, err)
	}
	defer db.Close()

	server := web.NewServer(db)

	http.HandleFunc("/get", server.GetHandler)
	http.HandleFunc("/set", server.SetHandler)

	log.Fatal(server.ListenAndServe(*httpAddr))
}
