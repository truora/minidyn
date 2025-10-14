package core

import "github.com/truora/minidyn/types"

// EnableStream enables DynamoDB Streams on the table
func (t *Table) EnableStream(viewType types.StreamViewType) {
	t.StreamEnabled = true
	t.StreamViewType = viewType
}

// DisableStream disables DynamoDB Streams on the table
func (t *Table) DisableStream() {
	t.StreamEnabled = false
}

// GetStreamRecords returns all stream records
func (t *Table) GetStreamRecords() []types.StreamRecord {
	return t.StreamRecords
}

// ClearStreamRecords clears all stream records
func (t *Table) ClearStreamRecords() {
	t.StreamRecords = []types.StreamRecord{}
}

// IsStreamEnabled returns whether streams are enabled
func (t *Table) IsStreamEnabled() bool {
	return t.StreamEnabled
}

// GetStreamViewType returns the current stream view type
func (t *Table) GetStreamViewType() types.StreamViewType {
	return t.StreamViewType
}
