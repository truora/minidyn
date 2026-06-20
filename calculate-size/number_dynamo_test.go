package docsize

import "testing"

func TestDynamoNumberSizeBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"0", 1},
		{"", 21},
		{"not-a-number", 21},
		{"5", 2},
		{"-5", 3},
		{"10", 2},
		{"123", 3},
		{"3.14159", 5},
		{"  42  ", 2}, // trimmed before parsing
		{"1234567890123456789012345678901234567890", 21}, // 21-byte cap
	}

	for _, c := range cases {
		if got := dynamoNumberSizeBytes(c.in); got != c.want {
			t.Errorf("dynamoNumberSizeBytes(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
