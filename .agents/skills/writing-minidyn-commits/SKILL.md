---
name: writing-minidyn-commits
description: Drafts Git commit messages for Minidyn: imperative subject, mandatory Why then What body (Why prose; What as hyphen bullets), lines wrapped at 80 columns, Issue footer; asks for motivation or issue number when unknown. Use when staging changes, writing commits, or when the user asks for a commit message or commit format.
---

# Writing Minidyn commits

## Conventions

**Subject line**

- **Imperative mood**, present tense: *Add*, *Fix*, *Update*, *Implement*, *Support*, *Remove*, *Migrate*, *Bump*, *Create*, *Return*, *Replace*, *Use*, etc.
- **Capitalize** the first word; keep the subject **80 characters or fewer** (hard limit).
- **Plain English** — prefer a short sentence over type prefixes (`feat:`, `fix:`); not required by history but allowed if the team agrees.
- **Scope by area** in the subject when it helps: e.g. package or feature (`Add batch get item to minidyn`).

**Body (mandatory)**

After a blank line following the subject, every commit **must** include **Why** first, then **What**. Each label ends with `:` on its own line; the content starts on the **next line** (newline after `:`).

```text
Why:
<motivation, context, or problem solved — why this change exists>

What:
- <concise point — what changed>
- <another point if needed>
```

**Line length (mandatory)**

- Every line of the **commit message** (subject, each line under **Why:**, each line under **What:**, and the **Issue:** footer) must be **at most 80 characters**, including spaces.
- Wrap prose at word boundaries; use multiple lines under **Why:** as needed. Under **What:**, each bullet is one line starting with `- `; keep the full line ≤80 (split into extra `-` lines if one point is too long). Do not rely on the viewer to soft-wrap.

Rules:

- **Why** is non-negotiable: do not omit it. If **Why** would only repeat **What**, sound generic (“improves code quality”), or require **guessing** the author’s intent, **stop and ask the user for the motivation** (problem triggered the change, constraint, bug symptom, or goal). After they answer, write **Why** from that. If the motivation is obvious from the diff alone, a short paragraph under **Why:** is enough (e.g. `Why:` then `Fixes incorrect Query pagination for partial pages.`).
- **What** restates the behavioral change (can mirror the subject with slightly more detail). Under **What:**, use **hyphen bullets only**: each non-empty line starts with `- ` (hyphen then space), then the point. Use one bullet for a single focused change, or several bullets when multiple distinct changes belong in the same commit. Do **not** use `*` or numbered lists (`1.`) under **What:**.
- **Bracket tags** (`[fix]`, `[Fix]`) remain optional in the subject only; do not replace What/Why.

**Footer (mandatory)**

After **Why** and **What**, add a **blank line**, then a single footer line with the issue reference:

```text
Issue: #<number>
```

- Use the tracker’s numeric id (e.g. GitHub/Jira/GitLab). Examples: `Issue: #42`, `Issue: #1234`.
- If the issue id is **unknown**, **ask the user** for it before finalizing the message (same as unclear **Why**).
- If the user confirms there is **no** tracking issue for this change, use `Issue: none` once they explicitly approve that.

**Do not**

- Mimic **Merge pull request #…** lines (those are GitHub merges, not hand-written messages).
- Use vague subjects (`Fix stuff`, `Update`) or a body that omits **Why:** or **What:** (or puts text on the same line as the label instead of after a newline).
- Exceed **80 characters** on any line of the commit message.
- Under **What:**, omit `-` bullets, put prose on the same line as **`What:`**, or use `*` / numbered lists instead of hyphens.
- Invent or guess **Why** when unsure — ask the user for motivation instead.
- Invent or guess the **Issue** id — ask the user or use `Issue: none` only after explicit confirmation.

## Workflow

1. Inspect `git diff` / staged files; group logically (one concern per commit when possible).
2. Choose the dominant verb (Add / Fix / Update / …) and the smallest accurate description.
3. **Always** add the mandatory body: **`Why:`** (with text on the following line), then **`What:`** (same). If **Why** is unclear from context, **ask the user for motivation** before proposing the final message.
4. Add the **footer** `Issue: #…` after a blank line. If the issue number is unknown, **ask the user** (or confirm `Issue: none`).
5. Before committing, re-read the subject as: **“This commit will _&lt;subject&gt;_”** (should read naturally) and confirm **Why**, **What**, and **Issue** are present and accurate; confirm every line is ≤ 80 characters and each line of **What** content starts with **`- `**.

## Examples (full message shape)

```text
Add clear-all support with error reuse

Why:
Lets callers reset state in tests without duplicating error-handling code.

What:
- Introduce clear-all behavior and reuse the same error path as other failures.

Issue: #100
```

```text
Fix nested attribute handling and value processing

Why:
Previous behavior dropped or misread nested values and broke Update/Query
expectations.

What:
- Correct how nested attributes are read and normalized in expressions.

Issue: #204
```

```text
Update golangci-lint version to v2.7

Why:
Keeps lint rules aligned with the supported toolchain and fixes upstream
deprecations.

What:
- Bump golangci-lint to v2.7 in CI and local config.

Issue: #88
```

## Output when the user asks for a message

In this section, use **hyphen bullets** (`-` at the start of each line) for the checklist below — not numbered lists.

Provide:

- **Proposed subject** (single line, ≤ 80 characters).
- **Mandatory body** (blank line after subject): **`Why:`** on its own line, motivation on the following line(s) (each line ≤ 80 characters), blank line, then **`What:`** on its own line, then one or more lines each starting with **`- `** (each full line ≤ 80 characters). If motivation is unknown or weak, **ask the user for it first**, then output the final **Why** from their answer.
- **Footer** after a blank line: `Issue: #<number>`, or `Issue: none` only if the user confirmed no ticket; if the id is unknown, **ask the user** first.
- **One-line note** if multiple commits were considered and squashed or split.
