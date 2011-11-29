package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"
)

const _max = 100000000

// dump a tsv of edges with maxval data stdout
func main() {
	var vertices, edges, offset int
	flag.IntVar(&vertices, "vertices", 1000, "number of vertices to generate")
	flag.IntVar(&edges, "edges", 1000, "number of edges on each vertex") // XXX should i make this a min/max and randomize?
	flag.IntVar(&offset, "offset", 0, "offset of first vertex id")
	flag.Parse()

	rand.Seed(time.Seconds())
	max := 0
	for i := offset; i < vertices+offset; i++ {
		value := rand.Intn(_max)
		fmt.Printf("%d\t%d\t", i, value)
		for j := 0; j < edges; j++ {
			fmt.Printf("%d\t", rand.Intn(vertices+offset))
		}
		if i > 0 {
			fmt.Printf("%d\t", i-1)
		}
		fmt.Println()
		if value > max {
			max = value
		}
	}
	fmt.Printf("# max: %d\n", max)
}
