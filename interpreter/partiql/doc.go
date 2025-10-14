// Package partiql provides a PartiQL (SQL-compatible query language) interpreter for DynamoDB.
//
// PartiQL is a SQL-compatible query language that makes it easier to interact with DynamoDB
// using familiar SQL syntax. This package provides lexing, parsing, and evaluation of PartiQL
// statements including:
//   - SELECT: Query and scan operations
//   - INSERT: Put item operations
//   - UPDATE: Update item operations
//   - DELETE: Delete item operations
//
// Example usage:
//
//	lexer := partiql.NewLexer("SELECT * FROM users WHERE id = ?")
//	parser := partiql.NewParser(lexer)
//	stmt := parser.ParseStatement()
//	evaluator := partiql.NewEvaluator([]interface{}{"user123"})
package partiql
