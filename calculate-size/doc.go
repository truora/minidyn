// Package docsize estimates DynamoDB item size in bytes for low-level DynamoDB JSON items
// (each attribute maps to a single-key AttributeValue: S, N, M, L, ...).
//
// The rules follow AWS documentation as summarized in
// https://zaccharles.medium.com/calculating-a-dynamodb-items-size-and-consumed-capacity-d1728942eb7c
// and match the reference implementation in
// https://github.com/zaccharles/dynamodb-calculator (index.html).
//
// This is not the same as treating {"S":"x"} as a DynamoDB Map (3 + key + payload + 1);
// older dynamodb-size-js style code did that and over-counts heavily on real exports.
package docsize
