package config

// Shard descirbes a shard that holds the appropriate set of keys.
// Each shard has unique set of keys.
type Shard struct {
	Name string
	Idx  int
}

// Config descirbes the sharding config.
type Config struct {
	Shard []Shard
}
