package language

import (
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Environment represents the execution enviroment
type Environment struct {
	store map[string]Object
}

// NewEnvironment creates a new enviroment
func NewEnvironment() *Environment {
	return &Environment{store: map[string]Object{}}
}

// AddAttributes adds the dynamodb attributes to the environment
func (e *Environment) AddAttributes(attributes map[string]*dynamodb.AttributeValue) error {
	for name, value := range attributes {
		obj, err := MapToObject(value)
		if err != nil {
			return err
		}

		e.Set(name, obj)
	}

	return nil
}

// Get gets the value of the variable in the environment
func (e *Environment) Get(name string) (Object, bool) {
	obj, ok := e.store[name]
	return obj, ok
}

// Set assigns the value of the variable in the environment
func (e *Environment) Set(name string, val Object) Object {
	e.store[name] = val
	return val
}

// Set assigns the value of the variable in the environment
func (e *Environment) String() string {
	out := []string{}

	for n, v := range e.store {
		out = append(out, n+" => "+v.Inspect())
	}

	sort.Strings(out)

	return "{" + strings.Join(out, ",") + "}"
}
