package main

import (
	"flag"
	"fmt"
	"os"
	"sort"

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
func RandomInt(min, max int64) int64 {
	return int64(Random()%uint64(max-min) + uint64(min))
}

func RandomString(min, max int64) string {
	size := RandomInt(min, max)
	buf := make([]byte, size)
	for i := 0; int64(i) < size; i++ {
		index := Random() % uint64(len(random_charset))
		buf[i] = random_charset[index]
	}
	return string(buf)
}

const (
	OP_Insert = iota
	OP_Update
	OP_Delete
)

func GetOP() int {
	val := Random() % 1000
	if val < 600 {
		return OP_Insert
	} else if val < 800 {
		return OP_Update
	} else if val < 1000 {
		return OP_Delete
	}
	return OP_Insert
}

func main() {
	var max_op int64
	flag.Int64Var(&max_op, "max-op", 100000, "default max operate count")
	flag.Parse()
	filename := fmt.Sprintf("chaos-golang")
	os.Remove(filename)
	// db, err := bolt.Open(filename, syscall.S_IRUSR|syscall.S_IWUSR|syscall.S_IRGRP|syscall.S_IWGRP|syscall.S_IROTH, nil)
	db, err := bolt.Open(filename, 0x100|0x80|0x20|0x10|0x04, nil)

	keys := make([]string, 0)
	// db.StrictMode = true

	if err != nil {
		fmt.Println(err.Error())
		return
	}
	bucket := RandomString(8, 32)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte(bucket))
		if err != nil {
			return err
		}
		for i := 0; int64(i) < max_op; i += 1 {
			op := GetOP()
			if op == OP_Insert {
				key := RandomString(8, 32)
				val := RandomString(32, 4096)
				keys = append(keys, key)
				fmt.Printf("%06d INSERT %s\n", i, key)
				err = b.Put([]byte(key), []byte(val))
				if err != nil {
					return err
				}
			} else if op == OP_Update {
				idx := RandomInt(0, int64(len(keys)))
				sort.Slice(keys, func(i, j int) bool {
					return keys[i] < keys[j]
				})
				key := keys[idx]
				val := RandomString(32, 4096)
				fmt.Printf("%06d UPDATE %s\n", i, key)
				err = b.Put([]byte(key), []byte(val))
				if err != nil {
					return err
				}
			} else if op == OP_Delete {
				idx := RandomInt(0, int64(len(keys)))
				sort.Slice(keys, func(i, j int) bool {
					return keys[i] < keys[j]
				})
				key := keys[idx]
				if idx == 0 {
					keys = keys[1:]
				} else if idx == int64(len(keys))-1 {
					keys = keys[:len(keys)-1]
				} else {
					keys = append(keys[:idx], keys[idx+1:]...)
				}
				fmt.Printf("%06d DELETE %s\n", i, key)
				err = b.Delete([]byte(key))
				if err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		fmt.Println(err.Error())
	}
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		val := b.Get([]byte("ZqReP8ryRa5y"))
		fmt.Printf("value: %s\n", string(val))
		return nil
	}); err != nil {

	}
	stat := db.Stats().TxStats
	fmt.Printf("PageCount: %d, PageAlloc: %d\n", stat.PageCount, stat.PageAlloc)
	fmt.Printf("CursorCount: %d\n", stat.CursorCount)
	fmt.Printf("NodeCount: %d, NodeDeref: %d\n", stat.NodeCount, stat.NodeDeref)
	fmt.Printf("Rebalance: %d, RebalanceTime: %s\n", stat.Rebalance, stat.RebalanceTime)
	fmt.Printf("Split: %d, Spill: %d, SpillTime: %s\n", stat.Split, stat.Spill, stat.SpillTime)
	fmt.Printf("Write: %d, WriteTime: %s\n", stat.Write, stat.WriteTime)
	db.Close()
}
