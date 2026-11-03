package server

// AttributeValue is a concrete representation of DynamoDB's AttributeValue
// that works with standard json encoding/decoding. L and M use omitzero so an
// empty list or map keeps its type (`"L":[]`, `"M":{}`) while a nil one is
// omitted.
type AttributeValue struct {
	B    []byte                     `json:"B,omitempty"`
	BOOL *bool                      `json:"BOOL,omitempty"`
	BS   [][]byte                   `json:"BS,omitempty"`
	L    []*AttributeValue          `json:"L,omitzero"`
	M    map[string]*AttributeValue `json:"M,omitzero"`
	N    *string                    `json:"N,omitempty"`
	NS   []*string                  `json:"NS,omitempty"`
	NULL *bool                      `json:"NULL,omitempty"`
	S    *string                    `json:"S,omitempty"`
	SS   []*string                  `json:"SS,omitempty"`
}
