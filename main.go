// only for learning. from  https://github.com/YuriyNasretdinov/distribkv
package main

import (
	"distribkv/db"
	"flag"
	"fmt"
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

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		key := r.Form.Get("key")

		value, err := db.GetKey(key)
		if err != nil {
			fmt.Fprintf(w, "error = %v", err)
			return
		}
		fmt.Fprintf(w, "value = %q", value)

	})

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		key := r.Form.Get("key")
		value := r.Form.Get("value")

		err := db.SetKey(key, []byte(value))

		fmt.Fprintf(w, "Error = %v", err)
	})

	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}
