package types

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
)

// ValidateItemAttributeValue walks an attribute value, including nested List (L) and Map (M)
// entries, and returns an error when any String Set (SS), Number Set (NS), or Binary Set (BS)
// contains duplicate members. The error message matches the DynamoDB ValidationException body:
// Callers typically wrap the result with NewError("ValidationException", msg, nil).
//
// Nil av is valid. NS duplicates are detected by exact wire string equality of each element
// (e.g. "1" and "1.0" are not duplicates). SS uses the same string comparison as StringValue.
// BS duplicates use bytes.Equal (nil and empty slices compare equal).
func ValidateItemAttributeValue(av *Item) error {
	if av == nil {
		return nil
	}

	if err := validateItemScalarSets(av); err != nil {
		return err
	}

	return validateItemNested(av)
}

func validateItemScalarSets(av *Item) error {
	if err := validateStringSet(av.SS); err != nil {
		return err
	}

	if err := validateNumberSet(av.NS); err != nil {
		return err
	}

	return validateBinarySet(av.BS)
}

func validateItemNested(av *Item) error {
	for _, elem := range av.L {
		if err := ValidateItemAttributeValue(elem); err != nil {
			return err
		}
	}

	for _, v := range av.M {
		if err := ValidateItemAttributeValue(v); err != nil {
			return err
		}
	}

	return nil
}

// ValidateItemMap runs ValidateItemAttributeValue on every value in m.
// Nil or empty maps are valid.
func ValidateItemMap(m map[string]*Item) error {
	if m == nil {
		return nil
	}

	for _, v := range m {
		if err := ValidateItemAttributeValue(v); err != nil {
			return err
		}
	}

	return nil
}

func duplicateCollectionError(displayParts []string) error {
	inner := strings.Join(displayParts, ", ")

	// Wording matches DynamoDB ValidationException; linters prefer generic Go error style.
	//nolint:revive,staticcheck // error-strings / ST1005 — message aligned with AWS.
	return fmt.Errorf("One or more parameter values were invalid: Input collection [%s] contains duplicates.", inner)
}

func validateStringSet(ss []*string) error {
	if len(ss) < 2 {
		return nil
	}

	parts := make([]string, len(ss))
	for i, p := range ss {
		parts[i] = StringValue(p)
	}

	seen := make(map[string]struct{}, len(parts))
	for _, v := range parts {
		if _, ok := seen[v]; ok {
			return duplicateCollectionError(parts)
		}

		seen[v] = struct{}{}
	}

	return nil
}

func validateNumberSet(ns []*string) error {
	if len(ns) < 2 {
		return nil
	}

	// Duplicates follow DynamoDB wire encoding: compared as the exact string sent for each N.
	parts := make([]string, len(ns))
	for i, p := range ns {
		parts[i] = StringValue(p)
	}

	seen := make(map[string]struct{}, len(parts))
	for _, v := range parts {
		if _, ok := seen[v]; ok {
			return duplicateCollectionError(parts)
		}

		seen[v] = struct{}{}
	}

	return nil
}

func validateBinarySet(bs [][]byte) error {
	if len(bs) < 2 {
		return nil
	}

	parts := make([]string, len(bs))
	for i, b := range bs {
		parts[i] = base64.StdEncoding.EncodeToString(b)
	}

	for i := range bs {
		for j := i + 1; j < len(bs); j++ {
			if bytes.Equal(bs[i], bs[j]) {
				return duplicateCollectionError(parts)
			}
		}
	}

	return nil
}
