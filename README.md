# LunarDB [![GoDoc](https://godoc.org/github.com/purehyperbole/lunar?status.svg)](https://godoc.org/github.com/purehyperbole/lunar) [![Go Report Card](https://goreportcard.com/badge/github.com/purehyperbole/lunar)](https://goreportcard.com/report/github.com/purehyperbole/lunar) [![Build Status](https://travis-ci.org/purehyperbole/lunar.svg?branch=master)](https://travis-ci.org/purehyperbole/lunar)

A simple embedded, persistent key value store for go.

The index makes use of a lock free radix tree, which is kept only in memory.

Data persistence is handled via a memory mapped file (MMAP).

# Motivation

This project was built for fun and learning. It probably has lots of bugs and shouldn't be used for any real workloads (yet!) 

# Installation

To start using lunar, you can run:

`$ go get github.com/purehyperbole/lunar`

# Usage

`Open` will open a database file. This will create a data and accompanying index file if the specified file(s) don't exist.

```go
package main

import (
    "github.com/purehyperbole/lunar"
)

func main() {
    // open a new or existing database file.
    db, err := lunar.Open("test.db")
    if err != nil {
        panic(err)
    }

    defer db.Close()
}
```

`Get` allows data to be retrieved.

```go
data, err := db.Get([]byte("myKey1234"))
```

`Set` allows data to be stored.

```go
err := db.Set([]byte("myKey1234"), []byte(`{"status": "ok"}`))
```

# Features/Wishlist

- [x] Persistence
- [x] Lock free index (Radix)
- [ ] Data file compaction
- [ ] Configurable sync on write options
- [ ] Transactions (MVCC)

## Versioning

For transparency into our release cycle and in striving to maintain backward
compatibility, this project is maintained under [the Semantic Versioning guidelines](http://semver.org/).

## Copyright and License

Code and documentation copyright since 2018 purehyperbole.

Code released under
[the MIT License](LICENSE).
