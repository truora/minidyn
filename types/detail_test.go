package types

import "testing"

func TestWithDetail(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		kv   []string
		want string
	}{
		{"no pairs keeps the message", "boom", nil, "boom"},
		{"one pair", "boom", []string{"field", "id"}, "boom | field: id"},
		{
			"several pairs",
			"An operand in the update expression has an incorrect data type",
			[]string{"operator", "DELETE", "target type", "SS", "operand type", "NS"},
			"An operand in the update expression has an incorrect data type | operator: DELETE; target type: SS; operand type: NS",
		},
		{"dangling key gets an empty value", "boom", []string{"a", "1", "b"}, "boom | a: 1; b: "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WithDetail(tt.msg, tt.kv...); got != tt.want {
				t.Errorf("WithDetail() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDetail(t *testing.T) {
	if got := Detail(); got != "" {
		t.Errorf("Detail() = %q, want empty", got)
	}

	if got := Detail("k", "v"); got != " | k: v" {
		t.Errorf("Detail(k, v) = %q", got)
	}
}
