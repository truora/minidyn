package server

// AttributeValue is a concrete representation of DynamoDB's AttributeValue
// that works with standard json encoding/decoding.
type AttributeValue struct {
	B    []byte                     `json:"B,omitempty"`
	BOOL *bool                      `json:"BOOL,omitempty"`
	BS   [][]byte                   `json:"BS,omitempty"`
	L    []*AttributeValue          `json:"L,omitempty"`
	M    map[string]*AttributeValue `json:"M,omitempty"`
	N    *string                    `json:"N,omitempty"`
	NS   []*string                  `json:"NS,omitempty"`
	NULL *bool                      `json:"NULL,omitempty"`
	S    *string                    `json:"S,omitempty"`
	SS   []*string                  `json:"SS,omitempty"`
}
