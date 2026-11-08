// Package capacity computes DynamoDB consumed-capacity values for minidyn operations.
//
// It is SDK-free and operates on minidyn's internal item form (map[string]*types.Item),
// so both the in-memory aws-v2 client and the HTTP server can share it. Item byte sizes
// come from the docsize package (calculate-size); this package turns those bytes into
// read/write capacity units following the documented DynamoDB rules (4KB per read unit,
// 1KB per write unit, eventual reads halved, transactional operations doubled).
package capacity
