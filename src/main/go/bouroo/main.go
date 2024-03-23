package main

import (
	"fmt"
	"time"
)

func main() {
	started := time.Now()
	run()
	fmt.Printf("%f", time.Since(started).Seconds())
}

func run() {
	fmt.Println("Hello, World!")
}
