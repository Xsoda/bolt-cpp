package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"syscall"

	bolt "github.com/boltdb/bolt"
)

var random_seed uint64 = 13
var random_charset string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func Random() uint64 {
	random_seed = random_seed*997 + 521
	return random_seed
}

func RandomCharset(length uint) string {
	var i uint
	val := make([]byte, length)
	for i = 0; i < length; i++ {
		index := Random() % uint64(len(random_charset))
		val[i] = random_charset[index]
	}
	return string(val)
}

func main() {
	filename := fmt.Sprintf("bolt-%s", RandomCharset(5))
	os.Remove(filename)
	db, err := bolt.Open(filename, syscall.S_IRUSR|syscall.S_IWUSR|syscall.S_IRGRP|syscall.S_IWGRP|syscall.S_IROTH, nil)
	// db.StrictMode = true
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	const count = 100000
	// keys := make([]string, 0)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for i := 0; i < count; i += 1 {
			// k := RandomCharset(8)
			// v := RandomCharset(100)
			// keys = append(keys, k)
			// if err := b.Put([]byte(k), []byte(v)); err != nil {
			// 	return err
			// }
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i))
			if err := b.Put(k, make([]byte, 100)); err != nil {
				return err
			}
		}
		if _, err := b.CreateBucket([]byte("sub")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		fmt.Println(err.Error())
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("widgets")).Cursor()
		// b := c.Bucket()
		// m := count / 2
		// sort.Slice(keys, func(i, j int) bool {
		// 	return strings.Compare(keys[i], keys[j]) < 0
		// })
		// bound := []byte(keys[m])
		bound := make([]byte, 8)
		binary.BigEndian.PutUint64(bound, uint64(count/2))
		// b.Dump()
		fmt.Printf("middle: %s\n", string(bound))
		for key, _ := c.First(); bytes.Compare(key, bound) < 0; key, _ = c.Next() {
			if err := c.Delete(); err != nil {
				fmt.Println(err.Error())
			}
		}

		k, v := c.Seek([]byte("sub"))
		fmt.Printf("key: %s, value: %s\n", string(k), string(v))
		if err := c.Delete(); err != bolt.ErrIncompatibleValue {
			fmt.Printf("unexpected error: %s\n", err.Error())
		}
		// b.Dump()
		return nil
	}); err != nil {
		fmt.Println(err.Error())
	}
	db.Close()
}
