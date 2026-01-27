package main

import (
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
	db.StrictMode = true
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		const count = 1000
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for i := 0; i < count; i += 1 {
			// k := RandomCharset(8)
			// v := RandomCharset(100)
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
	db.Close()
}
