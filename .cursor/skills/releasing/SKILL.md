---
name: releasing
description: Use when cutting a new release on this repository — generates Keep a Changelog notes from merged PRs, suggests the version bump, creates an annotated tag, and opens a draft GitHub release for human review.
---

# Releasing

Cut a new release on the current repository. Detect the next version from PRs merged since the last tag (or summarize project capabilities for a first release), draft notes, get explicit human approval, then create the tag and a draft GitHub release.

This skill operates only on the repository in the current working directory. It never publishes — releases go out as drafts so the user can verify on GitHub and click Publish manually.

## Quick path

1. Read state: last tag + PRs since (or all PRs if no tag exists)
2. Decide the path: incremental vs first release
3. Build the draft (categorize + bump, or capability summary)
4. Show the draft and ask: "ship as-is or change something?"
5. On approval: create the annotated tag, push, open a draft release
6. Print the draft URL

## Steps

### 1. Read state

```bash
last_tag=$(git describe --tags --abbrev=0 2>/dev/null || echo "")

if [ -n "$last_tag" ]; then
    last_date=$(git log -1 --format=%aI "$last_tag")
    gh pr list --state merged --base main \
        --search "merged:>=$last_date" \
        --json number,title,body,author,labels,mergedAt,url \
        --limit 100
else
    gh pr list --state merged --base main \
        --json number,title,body,author,labels,mergedAt,url \
        --limit 200
fi
```

If a tag exists and no PRs come back, **stop**: tell the user there is nothing to release since `<last_tag>`.

### 2. Decide the path

| Condition | Path |
|-----------|------|
| Previous tag exists | **Incremental** (§ 3a) |
| No tag, ≤ 10 PRs total | **Incremental** — treat history start as v0.0.0 |
| No tag, > 10 PRs total | **First release** (§ 3b) |

The 10-PR threshold is a heuristic, not a rule. If the repo has 12 PRs all from one week, incremental still works. If it has 8 PRs across 3 years for a mature internal tool, first release fits better. When in doubt, ask the user.

### 3a. Incremental flow

**The diff is the source of truth.** PR titles and bodies are hints — when they conflict with the diff (or omit something material), trust the code.

For each PR, gather inputs in this order:

1. **Title + body** (already in step 1)
2. **File list with stats** — always. Run:
   ```bash
   gh pr view <N> --json files,additions,deletions
   ```
3. **Full diff** — when title/body is vague, body is empty, or any red flag below fires:
   ```bash
   gh pr diff <N>
   ```

**Red flags that demand reading the diff:**

- Title says "Refactor", "Improve", "Update", or "Cleanup" but files in public surface (`types/`, exported package files, public API) changed
- Body is empty or a single line and the PR changed > 200 lines
- File list shows package directories deleted, or files renamed/moved across package boundaries
- `go.mod` / `go.sum` changes that aren't trivial version bumps

Categorize using these signals (combined title + diff):

| Signal | Category |
|--------|----------|
| Public package or exported symbol removed; OR title "Drop"/"Remove"/"Delete" confirmed by diff | Removed |
| Public type signature changed, struct field added/removed/renamed, or behavior of public function changed | Changed (flag breaking explicitly in the bullet) |
| New exported types/functions; OR title "Add"/"Implement"/"Support" confirmed by diff | Added |
| Bug fix referenced in title/body, with regression test added | Fixed |
| Dependency updates with CVE / GHSA references | Security |
| Dependency bumps without security context, internal refactors, tooling changes | Changed |
| `Deprecate` directive added or "Deprecate" in title | Deprecated |

Still ambiguous after reading the diff? Ask the user.

Suggest the version bump:

| Signal | Bump |
|--------|------|
| Anything in **Removed** or any breaking API change (post-1.0) | major |
| Anything in **Added** (new feature) | minor |
| Only **Fixed**, **Security**, or dependency updates | patch |

For pre-1.0 (`v0.x.y`), prefer **minor** over **major** even for breaking changes, unless the user explicitly opts into major.

Render notes per [Format](#format).

### 3b. First release flow

For repos with significant history but no prior tag. Don't enumerate every PR — produce a capability summary instead.

**The current state of the codebase is the source of truth**, not the PR history. PR titles can drift from what survived; the code on disk is what ships. Read the repo NOW; use PR titles only for cross-checking.

**Inputs to read:**

- `README.md` (and `docs/` if present) for the project's stated purpose and feature list
- Top-level directory listing — each top-level package usually maps to a functional area
- Exported symbols per package — for Go: `go doc ./<pkg>` or `grep -E '^(func [A-Z]|type [A-Z])'` over package files. For other languages: equivalent tooling
- PR titles from step 1 — used only to confirm capabilities mentioned in the README actually shipped

**Suggest the initial version** using maturity signals:

| Signals | Version |
|---------|---------|
| > 30 PRs, > 2 years active, README signals production use | v1.0.0 |
| 10–30 PRs, 6mo–2yr, internal tool with stable API | v0.x.0 (ask user for x) |
| README says "experimental" / "alpha" / "WIP" regardless of history | v0.x.0 |
| < 10 PRs, < 6 months | v0.1.0 |

The user always validates the version. These are heuristics, not rules.

**Render the notes** as a capability summary, not a PR enumeration:

```markdown
## [vX.Y.Z] - YYYY-MM-DD

Initial release of <project name>.

### Capabilities

- **<Functional area>** — one-sentence description of what's covered
- **<Functional area>** — one-sentence description
- **<Functional area>** — one-sentence description

### Notes

- Full development history: <link to closed PRs>
- See README for usage and supported features
```

Functional areas come from the codebase structure + README, not from forcing PRs into Keep a Changelog buckets. Pick names that map to how a user would describe the project, not how the codebase is organized internally.

### 4. Get approval (mandatory checkpoint)

Show the user:

- Proposed version: `vX.Y.Z`
- One-sentence reasoning for the bump (incremental) or for the version pick (first release)
- Full draft notes

Then ask, verbatim or close to it:

> "¿Lo lanzamos así, o querés cambiar algo? (versión / categorización / wording)"

Three valid responses:

| User says | Action |
|-----------|--------|
| "dale" / "ok" / explicit yes | Proceed to step 5 |
| Specific edit ("cambiá X", "el bump es minor", "movelo a Fixed") | Apply, re-show full draft, ask again |
| "no" / rejection | Stop. Do not tag |

**Never tag without an explicit yes for the current draft.** If the user edits anything, re-confirm the new draft.

### 5. Tag and release

```bash
notes_file=$(mktemp)
cat > "$notes_file" <<'EOF'
<approved notes content>
EOF

git tag -a "vX.Y.Z" -m "Release vX.Y.Z"
git push origin "vX.Y.Z"

gh release create "vX.Y.Z" \
    --draft \
    --title "vX.Y.Z" \
    --notes-file "$notes_file"
```

Print the draft release URL. The user opens it on GitHub and clicks Publish.

## Format

Keep a Changelog 1.1.0 for incremental releases. Use only sections with entries — drop empty ones. Each bullet is one sentence with the PR number.

```markdown
## [vX.Y.Z] - YYYY-MM-DD

### Added
- New capability description (#116)

### Changed
- Behavior change description (#114)

### Removed
- Removed surface description (#116)

### Fixed
- Bug fix description (#112)

### Security
- Vulnerability patch description (#113)
```

For first releases, see § 3b for the capability summary template.

## Edge cases

| Situation | Action |
|-----------|--------|
| Working tree dirty | Stop. Ask the user to commit or stash first |
| Not on `main` | Stop. State the expected branch and ask how to proceed |
| Tag already exists locally or remote | Stop. Ask before deleting + recreating |
| User rejects the draft | Discard. Do not tag, do not push |
| PR has no clear category | Read body; if still unclear, ask the user |
| Direct commits on main (not via PR) | List them under Changed using the commit subject |

## Checklist before tagging

- [ ] `git status` is clean
- [ ] On `main`, up to date with `origin/main`
- [ ] User explicitly approved this exact version number
- [ ] User explicitly approved this exact notes content
- [ ] Notes follow the format for the chosen path (Keep a Changelog or capability summary)

## Why draft, not publish

The draft step is the human safety net. The agent writes notes from PR titles, bodies, file lists, diffs, and (for first releases) the codebase state — it can still mislabel a "Fix" as a "Change" or miss a breaking implication that lives in a subtle type change. A draft release lets the user catch that on GitHub's preview before anything is public.

After 2-3 successful releases where the draft needed no edits, switch `--draft` to `--latest` in step 5 to auto-publish.
