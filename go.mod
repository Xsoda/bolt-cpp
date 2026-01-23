module bolt-test

go 1.24.3

require (
	github.com/boltdb/bolt v1.3.1
	golang.org/x/sys v0.40.0
)

replace github.com/boltdb/bolt => ./pkg/bolt
