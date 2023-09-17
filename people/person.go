package people

import (
	"bytes"
	"encoding/json"
	"io"
)

// create a struct of person
type Person struct {
	Name string
	Age  int
}

// create a method for person

func (p *Person) NewPerson(name string, age int) *Person {
	return &Person{
		Name: name,
		Age:  age,
	}
}

func (p *Person) ToJSON() io.Reader {
	// marshal and return io reader object
	jsonBytes, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return bytes.NewReader(jsonBytes)
}
