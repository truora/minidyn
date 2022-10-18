package language

import (
	"testing"

	"github.com/truora/minidyn/types"
)

func TestString(t *testing.T) {
	env := NewEnvironment()

	env.Set("foo", &String{Value: "blee"})
	env.Set("bar", &Number{Value: 10})

	if env.String() != "{bar => 10,foo => blee}" {
		t.Errorf("unexpected value. got=%v, want=%v", env.String(), "{bar => 10,foo => blee}")
	}
}

func TestApply(t *testing.T) {
	item := map[string]*types.Item{
		"a": {
			S: types.ToString("a"),
		},
	}

	env := NewEnvironment()
	env.Set(":fu", &String{Value: "blee"})
	env.Set("bar", &Number{Value: 10})

	env.Apply(item, map[string]string{":fu": "foo"}, map[string]bool{"bar": true})

	if len(item) != 2 {
		t.Errorf("Expected 3 items, got %d", len(item))
	}
}
