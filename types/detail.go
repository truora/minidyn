package types

import "strings"

// DetailSeparator separates DynamoDB's own error text from minidyn-only debug detail.
// DynamoDB never produces it, and the parity normalizer strips from its last occurrence.
const DetailSeparator = " | "

// Detail renders minidyn-only debug detail as " | key: value; key: value" from alternating
// keys and values. A dangling key gets an empty value. It returns "" without pairs, so it
// can be appended to a message (or to a %w wrapped error) unconditionally.
func Detail(kv ...string) string {
	if len(kv) == 0 {
		return ""
	}

	var b strings.Builder

	b.WriteString(DetailSeparator)

	for i := 0; i < len(kv); i += 2 {
		if i > 0 {
			b.WriteString("; ")
		}

		b.WriteString(kv[i])
		b.WriteString(": ")

		if i+1 < len(kv) {
			b.WriteString(kv[i+1])
		}
	}

	return b.String()
}

// WithDetail returns msg followed by minidyn-only debug detail (see Detail). msg must be
// DynamoDB's exact text.
func WithDetail(msg string, kv ...string) string {
	return msg + Detail(kv...)
}
