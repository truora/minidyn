package e2e

import "testing"

func TestNormalizeSDKErrorString(t *testing.T) {
	const prefix = "operation error DynamoDB: UpdateItem, https response error StatusCode: 400, "

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			"keeps DynamoDB's own semicolon part",
			prefix + "RequestID: abc, api error ValidationException: Invalid UpdateExpression: " +
				"Incorrect operand type for operator or function; operator: DELETE, operand type: STRING",
			prefix + "RequestID: , api error ValidationException: Invalid UpdateExpression: " +
				"Incorrect operand type for operator or function; operator: DELETE, operand type: STRING",
		},
		{
			"strips minidyn detail",
			"api error ValidationException: bad type | operator: DELETE; target type: SS",
			"api error ValidationException: bad type",
		},
		{
			"strips from the last separator so echoed input survives",
			"api error ValidationException: bad key: \"a | b\" | field: id",
			"api error ValidationException: bad key: \"a | b\"",
		},
		{
			"keeps the invalid key text",
			"api error ValidationException: ExpressionAttributeNames contains invalid key: Syntax error; key: \"#a-b\"",
			"api error ValidationException: ExpressionAttributeNames contains invalid key: Syntax error; key: \"#a-b\"",
		},
		{
			"cuts parse error detail (#188)",
			"api error ValidationException: Invalid ProjectionExpression: Syntax error; token: \"(\", near: \"(\"",
			"api error ValidationException: Invalid ProjectionExpression: Syntax error",
		},
		{
			"sorts the GSI not-project list (Local uses hash order)",
			"api error ValidationException: One or more parameter values were invalid: " +
				"Global secondary index gko does not project [zz, a, b]",
			"api error ValidationException: One or more parameter values were invalid: " +
				"Global secondary index gko does not project [a, b, zz]",
		},
		{
			"leaves a single name and other lists alone",
			"Global secondary index gko does not project [b]; Secondary index lko does not project one or more filter attributes: [b, a]",
			"Global secondary index gko does not project [b]; Secondary index lko does not project one or more filter attributes: [b, a]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeSDKErrorString(tt.in); got != tt.want {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}
