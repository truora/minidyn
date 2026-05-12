# Language Interpreter Support

Minidyn evaluates DynamoDB-style expressions for **ConditionExpression**, **FilterExpression**, **KeyConditionExpression**, **ProjectionExpression**, and **UpdateExpression**. Parsing and evaluation live under `interpreter/language`; the high-level API is `interpreter.Language` (`Match`, `Project`, `Update`).

Placeholders work like DynamoDB:

- **Expression attribute names** — identifiers prefixed with `#` (for example `#pk`). Resolve them via `ExpressionAttributeNames` / the corresponding alias maps in code.
- **Expression attribute values** — identifiers prefixed with `:` (for example `:v1`). Resolve them via `ExpressionAttributeValues`.

The grammar below is what the lexer/parser accept; **KeyConditionExpression** uses the same conditional grammar but DynamoDB only allows a restricted subset on keys (partition/sort). Minidyn relies on table/query logic for those constraints.

---

## Conditional expressions (Condition, Filter, Key)

Used for `Match` with `ExpressionTypeConditional`, `ExpressionTypeFilter`, or `ExpressionTypeKey`.

### Types

| Name      | Type     | Short | Supported? |
| --------- | -------- | ----- | ---------- |
| Number    | scalar   | N     | y          |
| String    | scalar   | S     | y          |
| Binary    | scalar   | B     | y          |
| Bool      | scalar   | BOOL  | y          |
| Null      | scalar   | NULL  | y          |
| List      | document | L     | y          |
| Map       | document | M     | y          |
| StringSet | set      | SS    | y          |
| NumberSet | set      | NS    | y          |
| BinarySet | set      | BS    | y          |

### Syntax

| Feature | Syntax | Notes |
| ------- | ------ | ----- |
| Comparators | `=`, `<>`, `<`, `<=`, `>`, `>=` | Ordering comparisons apply to **N**, **S**, and **B** only. |
| BETWEEN | `operand BETWEEN low AND high` | Low/high operands: **N**, **S**, or **B**. |
| IN | `operand IN (a, b, …)` | |
| Logical | `AND`, `OR`, `NOT` | |
| Parentheses | `( condition )` | Grouping |
| Functions | See below | |

### Functions (conditional context)

| Function | Supported? | Notes |
| -------- | ---------- | ----- |
| `attribute_exists(path)` | y | |
| `attribute_not_exists(path)` | y | |
| `attribute_type(path, type)` | y | Second argument is a DynamoDB type token (`S`, `N`, `L`, …). |
| `begins_with(path, substr)` | y | Path: **S** or **B**; substr must match (string or binary). |
| `contains(path, operand)` | y | Path: **S**, **B**, **L**, **SS**, **BS**, or **NS** (not **M**). Operand type must match `contains` rules for that container. |
| `size(path)` | partial | Only **S** and **B** paths return a length. Real DynamoDB also defines `size` for sets and lists; that behavior is **not** implemented here. |

---

## Projection expressions

Used for `Project` (`ProjectionExpression` in DynamoDB). This is **path-only** syntax: a comma-separated list of document paths. It does **not** accept update clauses (`SET`, `REMOVE`, …) or function calls.

### Supported path forms

| Form | Example | Supported? |
| ---- | ------- | ---------- |
| Top-level name | `ProductId`, `#attr` | y |
| Nested map segments | `a.b.c` | y |
| List index (literal) | `items[0]` | y |
| List index (dynamic) | `items[k]` | y if `k` is a placeholder or name that evaluates to a **number** at evaluation time |
| Comma-separated paths | `a, b.c, items[0]` | y |

Rules:

- Each segment after `.` is an identifier (or `#` placeholder).
- `[` … `]` selects a list index; map keys use `.name` rather than `[name]`.
- If a path evaluates to a missing attribute, that path is skipped (no error); the projected map only includes paths that resolved.

---

## Update expressions

Used for `Update` (`UpdateExpression`). Clauses are **`SET`**, **`REMOVE`**, **`ADD`**, and **`DELETE`**. You can repeat clauses (for example `SET … REMOVE …`) and list multiple actions after one keyword where the grammar allows.

### Clause overview

| Clause | Syntax pattern | Supported? |
| ------ | -------------- | ---------- |
| SET | `SET path = value [, path = value] …` | y |
| REMOVE | `REMOVE path [, path] …` | y |
| ADD | `ADD path value [, path value] …` | y |
| DELETE | `DELETE path value [, path value] …` | y |

### SET value features

- **Arithmetic** in value position: `+` and `-` between **numbers** (including placeholders that evaluate to numbers).
- **Nested assignment**: map keys and list indexes on the left-hand side (for example `map.k = :x`, `list[0] = :y`).
- **Functions in updates** — only these are allowed in update value expressions:

| Function | Supported? |
| -------- | ---------- |
| `if_not_exists(path, value)` | y |
| `list_append(list1, list2)` | y |

Other functions (`size`, `attribute_exists`, `begins_with`, `contains`, …) are **not** valid inside update values; they are rejected as “not allowed in an update expression.”

### ADD / DELETE

- **ADD**: numeric addition, or set union for **SS** / **NS** / **BS** (per DynamoDB rules).
- **DELETE**: removes elements from **SS** / **NS** / **BS**.

---

## Native interpreter overrides

If built-in evaluation is wrong or incomplete for your case, the **native interpreter** lets you register custom matchers or updaters (see the main `README.md` “Developer notes” / native interpreter section). That path bypasses the tables above for the expressions you override.
