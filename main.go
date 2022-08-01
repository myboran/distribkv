// only for learning. from  https://github.com/YuriyNasretdinov/distribkv
package main

import (
	"distribkv/config"
	"distribkv/db"
	"distribkv/replication"
	"distribkv/web"
	"flag"
	"log"
	"net/http"
)

var (
	dbLocation = flag.String("db-location", "", "the path to the bolt db database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard for the data")
	replica    = flag.Bool("replica", false, "Whether or not run as a read-only replica")
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

	c, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config %q: %v", *configFile, err)
	}

	shards, err := config.ParseShards(c.Shards, *shard)
	if err != nil {
		log.Fatalf("Error parsing shards config %v", err)
	}

	log.Printf("Shard count is %d, current shard: %d", shards.Count, shards.CurIdx)

	dbs, err := db.NewDatabase(*dbLocation)
	if err != nil {
		log.Fatalf("NewDatabase (%q) err: %v", *dbLocation, err)
	}
	defer dbs.Close()

	if *replica {
		leaderAddr, ok := shards.Addrs[shards.CurIdx]
		if !ok {
			log.Fatalf("Could not find address for leader for shard %d", shards.CurIdx)
		}
		go replication.ClientLoop(dbs, leaderAddr)
	}

	srv := web.NewServer(dbs, shards)
	http.HandleFunc("/get", srv.GetHandler)
	http.HandleFunc("/set", srv.SetHandler)
	http.HandleFunc("/next-replication-key", srv.GetNextKeyForReplication)
	http.HandleFunc("/delete-replication-key", srv.DeleteReplicationKey)
	log.Fatal(srv.ListenAndServe(*httpAddr))
}
