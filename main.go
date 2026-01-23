package main

import (
	"bytes"
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
	os.Remove("cache")
	db, err := bolt.Open("cache", syscall.S_IRUSR|syscall.S_IWUSR|syscall.S_IRGRP|syscall.S_IWGRP|syscall.S_IROTH, nil)
	db.StrictMode = true
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	// if err := db.Update(func(tx *bolt.Tx) error {
	// 	_, err := tx.CreateBucket([]byte("widgets"))
	// 	if err != nil {
	// 		return err
	// 	}
	// 	return nil
	// }); err != nil {
	// 	fmt.Println(err.Error())
	// 	return
	// }
	// tx, err := db.Begin(true)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return
	// }
	// if err := tx.Commit(); err != nil {
	// 	fmt.Println(err.Error())
	// 	return
	// }
	// if err := tx.DeleteBucket([]byte("foo")); err != bolt.ErrTxClosed {
	// 	fmt.Println(err.Error())
	// 	return
	// }
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			fmt.Println(err.Error())
		}
		if err := b.Put([]byte("foo"), []byte("0001")); err != nil {
			fmt.Println(err.Error())
		}
		if err := b.Put([]byte("bar"), []byte("0002")); err != nil {
			fmt.Println(err.Error())
		}
		if err := b.Put([]byte("baz"), []byte("0003")); err != nil {
			fmt.Println(err.Error())
		}

		if _, err := b.CreateBucket([]byte("bkt")); err != nil {
			fmt.Println(err.Error())
		}
		return nil
	}); err != nil {
		fmt.Println(err.Error())
	}
	if err := db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("widgets")).Cursor()

		// Exact match should go to the key.
		if k, v := c.Seek([]byte("bar")); !bytes.Equal(k, []byte("bar")) {
			fmt.Printf("unexpected key: %v\n", k)
		} else if !bytes.Equal(v, []byte("0002")) {
			fmt.Printf("unexpected value: %v\n", v)
		}
		// Inexact match should go to the next key.
		if k, v := c.Seek([]byte("bas")); !bytes.Equal(k, []byte("baz")) {
			fmt.Printf("unexpected key: %v\n", k)
		} else if !bytes.Equal(v, []byte("0003")) {
			fmt.Printf("unexpected value: %v\n", v)
		}
		return nil
		// Low key should go to the first key.
		if k, v := c.Seek([]byte("")); !bytes.Equal(k, []byte("bar")) {
			fmt.Printf("unexpected key: %v\n", k)
		} else if !bytes.Equal(v, []byte("0002")) {
			fmt.Printf("unexpected value: %v\n", v)
		}

		// High key should return no key.
		if k, v := c.Seek([]byte("zzz")); k != nil {
			fmt.Printf("expected nil key: %v\n", k)
		} else if v != nil {
			fmt.Printf("expected nil value: %v\n", v)
		}

		// Buckets should return their key but no value.
		if k, v := c.Seek([]byte("bkt")); !bytes.Equal(k, []byte("bkt")) {
			fmt.Printf("unexpected key: %v\n", k)
		} else if v != nil {
			fmt.Printf("expected nil value: %v\n", v)
		}

		return nil
	}); err != nil {
		fmt.Println(err.Error())
	}
	db.Close()
}
