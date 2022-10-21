package language

import (
	"sort"
	"strings"

	"github.com/ldelacruztruora/minidyn/types"
)

// Environment represents the execution enviroment
type Environment struct {
	store     map[string]Object
	Aliases   map[string]string
	toCompact []Object
}

// NewEnvironment creates a new enviroment
func NewEnvironment() *Environment {
	return &Environment{store: map[string]Object{}, Aliases: map[string]string{}, toCompact: []Object{}}
}

// AddAttributes adds the types attributes to the environment
func (e *Environment) AddAttributes(attributes map[string]*types.Item) error {
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
func (e *Environment) Get(name string) Object {
	n := name

	if alias, ok := e.Aliases[n]; ok {
		n = alias
	}

	obj, ok := e.store[n]
	if ok {
		return obj
	}

	// support index notation
	names := e.evalNameWithIndex(n)

	size := len(names)
	if size == 0 {
		return NULL
	}

	return e.getFromIndexes(names, size)
}

func (e *Environment) evalNameWithIndex(name string) []string {
	names := strings.Split(name, ".")
	for _, n := range names {
		if alias, ok := e.Aliases[n]; ok {
			names = append(names, e.evalNameWithIndex(alias)...)
		}
	}

	return names
}

func (e *Environment) getFromIndexes(names []string, size int) Object {
	var obj Object

	ok := false

	obj, ok = e.store[names[0]]
	if !ok {
		return NULL
	}

	for i, n := range names[1:] {
		if alias, ok := e.Aliases[n]; ok {
			n = alias
		}

		obj = getFromMap(obj, n)

		if i+2 == size {
			break
		}

		if isError(obj) {
			return obj
		}
	}

	return obj
}

func getFromMap(obj Object, key string) Object {
	m, ok := obj.(*Map)
	if !ok {
		return newError("index operator not supported for %q", obj.Type())
	}

	return m.Get(key)
}

// Set assigns the value of the variable in the environment
func (e *Environment) Set(name string, val Object) Object {
	n := name

	if alias, ok := e.Aliases[n]; ok {
		n = alias
	}

	e.store[n] = val

	return val
}

// Remove remove name from the environment
func (e *Environment) Remove(name string) {
	n := name

	if alias, ok := e.Aliases[name]; ok {
		n = alias
	}

	_, ok := e.store[n]
	if ok {
		delete(e.store, n)

		return
	}
}

// MarkToCompact adds the modified object to the list of objects that must be compact
func (e *Environment) MarkToCompact(obj Object) {
	e.toCompact = append(e.toCompact, obj)
}

// Compact removes extra information from the modified objects
func (e *Environment) Compact() {
	for _, obj := range e.toCompact {
		list, ok := obj.(*List)
		if !ok {
			continue
		}

		list.Compact()
	}
}

// Apply assigns the environment field to the item
func (e *Environment) Apply(item map[string]*types.Item, aliases map[string]string, exclude map[string]bool) {
	for k, v := range e.store {
		if _, ok := exclude[k]; ok {
			continue
		}

		if alias, ok := aliases[k]; ok {
			k = alias
		}

		vItem := v.ToDynamoDB()
		item[k] = &vItem
	}
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
