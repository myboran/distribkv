// only for learning. from  https://github.com/YuriyNasretdinov/distribkv
package main

import (
	"distribkv/config"
	"distribkv/db"
	"distribkv/web"
	"flag"
	"log"
	"net/http"

	"github.com/BurntSushi/toml"
)

var (
	dbLocation = flag.String("db-location", "", "the path to the bolt db database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard for the data")
)

func parseFlags() {
	flag.Parse()

	if *dbLocation == "" {
		log.Fatal("Must provide db-location")
	}
	if *shard == "" {
		log.Fatal("Must provide shard")
	}
}

func main() {
	parseFlags()

	var c config.Config
	if _, err := toml.DecodeFile(*configFile, &c); err != nil {
		log.Fatalf("toml.DecodeFile(%q): %v", *configFile, err)
	}

	var (
		shardCount int
		shardIdx   int = -1
	)
	shardCount = len(c.Shard)
	for _, s := range c.Shard {
		if s.Name == *shard {
			shardIdx = s.Idx
		}

	}
	if shardIdx < 0 {
		log.Fatalf("shard %q was not found.", *shard)
	}
	log.Printf("Shard count is %d, current shard: %d", shardCount, shardIdx)

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
