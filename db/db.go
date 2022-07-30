package db

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var defaultBucker = []byte("default")

// Database is a bolt database.
type Database struct {
	db *bolt.DB
}

// NewDatabase returns an instance of a database that we can work with.
func NewDatabase(dbPath string) (*Database, error) {
	blotDb, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}
	db := &Database{db: blotDb}
	if err := db.createDefaultBucket(); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating default bucket err: %w", err)
	}
	return db, nil
}

// Close can close this database.
func (d *Database) Close() error {
	return d.db.Close()
}

func (d *Database) createDefaultBucket() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultBucker)
		return err
	})
}

// SetKey set the key to the requested value into the default or return an error.
func (d *Database) SetKey(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucker)
		return b.Put([]byte(key), value)
	})
}

// GetKey get the value of the requested from a default database.
func (d *Database) GetKey(key string) ([]byte, error) {
	var value []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucker)
		value = b.Get([]byte(key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}
