package executor

import (
	"fmt"
	"math/rand"
	"time"

	. "gopkg.in/check.v1"
)

func (s *CGOTests) SetUpSuite(c *C) {
}

func (s *CGOTests) TearDownSuite(c *C) {
}

func (s *CGOTests) TestQuickSort(c *C) {
	type Record struct {
		a, b   int64
		c, key uint32
	}

	testarr := []Record{
		Record{1, 10, 0, 1},
		Record{2, 20, 0, 2},
		Record{3, 30, 0, 3},
		Record{4, 40, 0, 4},
		Record{9, 90, 0, 9},
		Record{5, 50, 0, 5},
		Record{6, 60, 0, 6},
		Record{7, 70, 0, 7},
		Record{8, 80, 0, 8},
		Record{1, 10, 0, 1},
	}
	QuickSortKeyAtEndUINT32(testarr)
	c.Assert(testarr[9].key, Equals, uint32(9))
	c.Assert(testarr[0].key, Equals, uint32(1))
	c.Assert(testarr[1].key, Equals, uint32(1))
	c.Assert(testarr[1].b, Equals, int64(10))
}

func (s *CGOTests) TestQuickSortSpeed(c *C) {
	type Record struct {
		a      int64
		c, key uint32
	}
	array := make([]Record, 1000)
	for i := 0; i < 1000; i++ {
		array[i].key = uint32(rand.Int31n(999999999))
	}
	start := time.Now()
	ops := 1000
	for i := 0; i < ops; i++ {
		QuickSortKeyAtEndUINT32(array)
	}

	elapsed := time.Now().Sub(start).Nanoseconds() / int64(ops)

	fmt.Printf("[QUICKSORT] %v ns/op\n", elapsed)
}

func (s *CGOTests) TestTimSort(c *C) {
	type Record struct {
		a, b   int64
		c, key uint32
	}

	testarr := []Record{
		Record{1, 10, 0, 1},
		Record{2, 20, 0, 2},
		Record{3, 30, 0, 3},
		Record{4, 40, 0, 4},
		Record{9, 90, 0, 9},
		Record{5, 50, 0, 5},
		Record{6, 60, 0, 6},
		Record{7, 70, 0, 7},
		Record{8, 80, 0, 8},
		Record{1, 10, 0, 1},
	}
	TimSortUINT32(testarr)
	c.Assert(testarr[9].key, Equals, uint32(9))
	c.Assert(testarr[0].key, Equals, uint32(1))
	c.Assert(testarr[1].key, Equals, uint32(1))
	c.Assert(testarr[1].b, Equals, int64(10))
}

func (s *CGOTests) TestTimSortSpeed(c *C) {
	type Record struct {
		a      int64
		c, key uint32
	}
	array := make([]Record, 1000)
	for i := 0; i < 1000; i++ {
		array[i].key = uint32(rand.Int31n(999999999))
	}
	start := time.Now()
	ops := 1000
	for i := 0; i < ops; i++ {
		TimSortUINT32(array)
	}

	elapsed := time.Now().Sub(start).Nanoseconds() / int64(ops)

	fmt.Printf("[TIMSORT] %v ns/op\n", elapsed)
}
