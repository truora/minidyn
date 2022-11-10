package interpreter

import (
	"errors"
	"testing"

	"github.com/truora/minidyn/types"
)

var boolTrue = true

func TestNativeMatch(t *testing.T) {
	item := map[string]*types.Item{
		"a": {
			S: types.ToString("a"),
		},
		"n": {
			N: types.ToString("1"),
		},
		"b": {
			BOOL: &boolTrue,
		},
		"txt": {
			S: types.ToString("hello world"),
		},
	}

	input := MatchInput{
		TableName:      "test",
		Expression:     ":a = a",
		Item:           item,
		ExpressionType: ExpressionTypeConditional,
		Attributes: map[string]*types.Item{
			":a": {
				S: types.ToString("a"),
			},
		},
	}

	native := NewNativeInterpreter()

	_, err := native.Match(input)
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Error("match without a defined expression should fail")
	}

	native.AddMatcher("test", ExpressionTypeConditional, ":a = a", func(m1, m2 map[string]*types.Item) bool {
		return true
	})

	matched, err := native.Match(input)
	if err != nil {
		t.Error("match with a defined expression should not fail")
	}

	if !matched {
		t.Error("input should match")
	}
}

func TestAddMatcher(t *testing.T) {
	native := NewNativeInterpreter()
	etypes := []ExpressionType{
		ExpressionTypeConditional,
		ExpressionTypeFilter,
		ExpressionTypeKey,
	}

	for _, etype := range etypes {
		native.AddMatcher("test", etype, ":a = a", func(m1, m2 map[string]*types.Item) bool {
			return true
		})
	}

	if len(native.keyExpressions) != 1 {
		t.Errorf("key expression should be 1 but got %d", len(native.keyExpressions))
	}

	if len(native.filterExpressions) != 1 {
		t.Errorf("filter expression should be 1 but got %d", len(native.keyExpressions))
	}

	if len(native.writeCondExpressions) != 1 {
		t.Errorf("write conditional expression should be 1 but got %d", len(native.keyExpressions))
	}

	for _, etype := range etypes {
		matcher, err := native.getMatcher("test", ":a = a", etype)
		if err != nil {
			t.Errorf("get %s should not fail but got %s error", etype, err)
		}

		if matcher == nil {
			t.Errorf("expression %s should be found", etype)
		}
	}
}

func TestNativeUpdate(t *testing.T) {
	item := map[string]*types.Item{
		"a": {
			S: types.ToString("a"),
		},
		"n": {
			N: types.ToString("1"),
		},
		"b": {
			BOOL: &boolTrue,
		},
		"txt": {
			S: types.ToString("hello world"),
		},
	}

	input := UpdateInput{
		TableName:  "test",
		Expression: "set a = :b",
		Item:       item,
		Attributes: map[string]*types.Item{
			":b": {
				S: types.ToString("foo"),
			},
		},
	}

	native := NewNativeInterpreter()

	err := native.Update(input)
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Error("update without a defined expression should fail")
	}

	native.AddUpdater("test", "set a = :b", func(m1, m2 map[string]*types.Item) {
		m1["a"] = m2[":b"]
	})

	err = native.Update(input)
	if err != nil {
		t.Error("match with a defined expression should not fail")
	}

	if types.StringValue(item["a"].S) != "foo" {
		t.Error("item should have been updated")
	}
}
