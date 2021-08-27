package language

import (
	"testing"
)

func TestString(t *testing.T) {
	env := NewEnvironment()

	env.Set("foo", &String{Value: "blee"})
	env.Set("bar", &Number{Value: 10})

	if env.String() != "{bar => 10,foo => blee}" {
		t.Errorf("unexpected value. got=%v, want=%v", env.String(), "{bar => 10,foo => blee}")
	}
}
