package db

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var defaultBucket = []byte("default")
var replicaBucket = []byte("replication")

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
	if err := db.createBucket(); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating default bucket err: %w", err)
	}
	return db, nil
}

// Close can close this database.
func (d *Database) Close() error {
	return d.db.Close()
}

func (d *Database) createBucket() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(replicaBucket); err != nil {
			return err
		}
		return nil
	})
}

// SetKey set the key to the requested value into the default or return an error.
func (d *Database) SetKey(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(defaultBucket).Put([]byte(key), value); err != nil {
			return err
		}
		return tx.Bucket(replicaBucket).Put([]byte(key), value)
	})
}

// SetKeyOnReplica sets the key to the requested value into the default database and does not write to the replication queue.
// This method is intended to be used only on replicas.
func (d *Database) SetKeyOnReplica(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(defaultBucket).Put([]byte(key), value)
	})
}

// GetKey get the value of the requested from a default database.
func (d *Database) GetKey(key string) ([]byte, error) {
	var value []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		value = b.Get([]byte(key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func copyByteSlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}

func (d *Database) GetNextKeyForReplication() (key, value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)
		k, v := b.Cursor().First()
		key = copyByteSlice(k)
		value = copyByteSlice(v)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return key, value, err
}

func (d *Database) DeleteReplicationKey(key, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)

		v := b.Get(key)
		if v == nil {
			return fmt.Errorf("key does not exist")
		}
		if !bytes.Equal(v, value) {
			return fmt.Errorf("value does not match")
		}
		return b.Delete(key)
	})
}

func (d *Database) DeleteExtraKeys(isExtra func(string) bool) error {
	var keys []string

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.ForEach(func(k, v []byte) error {
			ks := string(k)
			if isExtra(ks) {
				keys = append(keys, ks)
			}
			return nil
		})
	})
	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		for _, k := range keys {
			if err = b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
}
