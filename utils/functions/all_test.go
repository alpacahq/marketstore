package functions

import (
	"fmt"
	"github.com/alpacahq/marketstore/utils/io"
	. "gopkg.in/check.v1"
	"testing"
)

// Hook up gocheck into the "go test" runner.
var _ = Suite(&TestSuite{})

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

func (s *TestSuite) SetUpSuite(c *C) {
}

func (s *TestSuite) TearDownSuite(c *C) {
}

func (s *TestSuite) TestParameters(c *C) {
	var requiredColumns = []io.DataShape{
		{Name: "A", Type: io.FLOAT32},
		{Name: "B", Type: io.FLOAT32},
		{Name: "C", Type: io.FLOAT32},
		{Name: "D", Type: io.FLOAT32},
	}
	var optionalColumns = []io.DataShape{
		{Name: "E", Type: io.FLOAT32},
		{Name: "F", Type: io.FLOAT32},
	}

	/*
		All columns positionally specified
	*/
	argMap := NewArgumentMap(requiredColumns)
	idList := []string{"A::i", "B::j", "C::k", "D::l"}
	err := argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	c.Assert(err == nil, Equals, true)
	c.Assert(argMap.nameMap["A"][0].Name, Equals, "i")
	c.Assert(argMap.nameMap["B"][0].Name, Equals, "j")
	c.Assert(argMap.nameMap["C"][0].Name, Equals, "k")
	c.Assert(argMap.nameMap["D"][0].Name, Equals, "l")

	/*
		All columns positionally specified with optionals
	*/
	argMap = NewArgumentMap(requiredColumns, optionalColumns...)
	idList = []string{"A::i", "B::j", "C::k", "D::l", "E::m", "F::n"}
	err = argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	c.Assert(err == nil, Equals, true)
	c.Assert(argMap.nameMap["A"][0].Name, Equals, "i")
	c.Assert(argMap.nameMap["B"][0].Name, Equals, "j")
	c.Assert(argMap.nameMap["C"][0].Name, Equals, "k")
	c.Assert(argMap.nameMap["D"][0].Name, Equals, "l")
	c.Assert(argMap.nameMap["E"][0].Name, Equals, "m")
	c.Assert(argMap.nameMap["F"][0].Name, Equals, "n")

	/*
		Mixed positional and named
	*/
	argMap = NewArgumentMap(requiredColumns)
	idList = []string{"A::i", "j", "k", "D::l"}
	err = argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	c.Assert(err == nil, Equals, true)
	c.Assert(argMap.nameMap["A"][0].Name, Equals, "i")
	c.Assert(argMap.nameMap["B"][0].Name, Equals, "j")
	c.Assert(argMap.nameMap["C"][0].Name, Equals, "k")
	c.Assert(argMap.nameMap["D"][0].Name, Equals, "l")

	/*
		Multiple inputs mapped to single
	*/
	argMap = NewArgumentMap(requiredColumns)
	idList = []string{"A::i1", "A::i2", "j", "k", "D::l1", "D::l2", "D::l3"}
	err = argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	c.Assert(err == nil, Equals, true)
	c.Assert(argMap.nameMap["A"][0].Name, Equals, "i1")
	c.Assert(argMap.nameMap["A"][1].Name, Equals, "i2")
	c.Assert(argMap.nameMap["B"][0].Name, Equals, "j")
	c.Assert(argMap.nameMap["C"][0].Name, Equals, "k")
	c.Assert(argMap.nameMap["D"][0].Name, Equals, "l1")
	c.Assert(argMap.nameMap["D"][1].Name, Equals, "l2")
	c.Assert(argMap.nameMap["D"][2].Name, Equals, "l3")

	/*
		Multiple inputs mapped to single with optional columns included
	*/
	argMap = NewArgumentMap(requiredColumns, optionalColumns...)
	idList = []string{"A::i1", "A::i2", "j", "k", "D::l1", "D::l2", "D::l3", "m", "n"}
	err = argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	c.Assert(err == nil, Equals, true)
	c.Assert(argMap.nameMap["A"][0].Name, Equals, "i1")
	c.Assert(argMap.nameMap["A"][1].Name, Equals, "i2")
	c.Assert(argMap.nameMap["B"][0].Name, Equals, "j")
	c.Assert(argMap.nameMap["C"][0].Name, Equals, "k")
	c.Assert(argMap.nameMap["D"][0].Name, Equals, "l1")
	c.Assert(argMap.nameMap["D"][1].Name, Equals, "l2")
	c.Assert(argMap.nameMap["D"][2].Name, Equals, "l3")
	c.Assert(argMap.nameMap["E"][0].Name, Equals, "m")
	c.Assert(argMap.nameMap["F"][0].Name, Equals, "n")

	/*
		Insufficient params (error)
	*/
	argMap = NewArgumentMap(requiredColumns)
	idList = []string{"A::i", "j", "D::l"}
	err = argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	c.Assert(err != nil, Equals, true)
}
