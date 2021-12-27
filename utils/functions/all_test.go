package functions

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

func TestParameters(t *testing.T) {
	t.Parallel()
	requiredColumns := []io.DataShape{
		{Name: "A", Type: io.FLOAT32},
		{Name: "B", Type: io.FLOAT32},
		{Name: "C", Type: io.FLOAT32},
		{Name: "D", Type: io.FLOAT32},
	}
	optionalColumns := []io.DataShape{
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
	assert.Nil(t, err)
	assert.Equal(t, argMap.nameMap["A"][0].Name, "i")
	assert.Equal(t, argMap.nameMap["B"][0].Name, "j")
	assert.Equal(t, argMap.nameMap["C"][0].Name, "k")
	assert.Equal(t, argMap.nameMap["D"][0].Name, "l")

	/*
		All columns positionally specified with optionals
	*/
	argMap = NewArgumentMap(requiredColumns, optionalColumns...)
	idList = []string{"A::i", "B::j", "C::k", "D::l", "E::m", "F::n"}
	err = argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	assert.Nil(t, err)
	assert.Equal(t, argMap.nameMap["A"][0].Name, "i")
	assert.Equal(t, argMap.nameMap["B"][0].Name, "j")
	assert.Equal(t, argMap.nameMap["C"][0].Name, "k")
	assert.Equal(t, argMap.nameMap["D"][0].Name, "l")
	assert.Equal(t, argMap.nameMap["E"][0].Name, "m")
	assert.Equal(t, argMap.nameMap["F"][0].Name, "n")

	/*
		Mixed positional and named
	*/
	argMap = NewArgumentMap(requiredColumns)
	idList = []string{"A::i", "j", "k", "D::l"}
	err = argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	assert.Nil(t, err)
	assert.Equal(t, argMap.nameMap["A"][0].Name, "i")
	assert.Equal(t, argMap.nameMap["B"][0].Name, "j")
	assert.Equal(t, argMap.nameMap["C"][0].Name, "k")
	assert.Equal(t, argMap.nameMap["D"][0].Name, "l")

	/*
		Multiple inputs mapped to single
	*/
	argMap = NewArgumentMap(requiredColumns)
	idList = []string{"A::i1", "A::i2", "j", "k", "D::l1", "D::l2", "D::l3"}
	err = argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	assert.Nil(t, err)
	assert.Equal(t, argMap.nameMap["A"][0].Name, "i1")
	assert.Equal(t, argMap.nameMap["A"][1].Name, "i2")
	assert.Equal(t, argMap.nameMap["B"][0].Name, "j")
	assert.Equal(t, argMap.nameMap["C"][0].Name, "k")
	assert.Equal(t, argMap.nameMap["D"][0].Name, "l1")
	assert.Equal(t, argMap.nameMap["D"][1].Name, "l2")
	assert.Equal(t, argMap.nameMap["D"][2].Name, "l3")

	/*
		Multiple inputs mapped to single with optional columns included
	*/
	argMap = NewArgumentMap(requiredColumns, optionalColumns...)
	idList = []string{"A::i1", "A::i2", "j", "k", "D::l1", "D::l2", "D::l3", "m", "n"}
	err = argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	assert.Nil(t, err)
	assert.Equal(t, argMap.nameMap["A"][0].Name, "i1")
	assert.Equal(t, argMap.nameMap["A"][1].Name, "i2")
	assert.Equal(t, argMap.nameMap["B"][0].Name, "j")
	assert.Equal(t, argMap.nameMap["C"][0].Name, "k")
	assert.Equal(t, argMap.nameMap["D"][0].Name, "l1")
	assert.Equal(t, argMap.nameMap["D"][1].Name, "l2")
	assert.Equal(t, argMap.nameMap["D"][2].Name, "l3")
	assert.Equal(t, argMap.nameMap["E"][0].Name, "m")
	assert.Equal(t, argMap.nameMap["F"][0].Name, "n")

	/*
		Insufficient params (error)
	*/
	argMap = NewArgumentMap(requiredColumns)
	idList = []string{"A::i", "j", "D::l"}
	err = argMap.PrepareArguments(idList)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	assert.NotNil(t, err)
}
