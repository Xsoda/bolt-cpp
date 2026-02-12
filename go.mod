module bolt-test

go 1.25.0

require github.com/boltdb/bolt v1.3.1

require golang.org/x/sys v0.41.0 // indirect

replace github.com/boltdb/bolt => ./pkg/bolt
