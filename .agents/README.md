# Agent configuration

This directory is the **canonical, editor-agnostic** home for AI agent configuration in this repository.

## Layout

```
.agents/
  rules/          # Always-on project rules (workflow, conventions)
  skills/         # Task-specific skills (SKILL.md per skill)
  agents/         # Sub-agent definitions (qa-engineer, etc.)
```

## Editor compatibility

| Editor / tool | How it reads this config |
|---------------|--------------------------|
| **Any** | Root [`AGENTS.md`](../AGENTS.md) points here |
| **Cursor** | Symlinks under `.cursor/` → `.agents/` |
| **Claude Code** | [`CLAUDE.md`](../CLAUDE.md) + symlinks under `.claude/` → `.agents/` |
| **GitHub Copilot** | [`.github/copilot-instructions.md`](../.github/copilot-instructions.md) |
| **Codex, Windsurf, etc.** | Native `AGENTS.md` support at repo root |

Do **not** edit files under `.cursor/` or `.claude/` directly — they are symlinks. Change content in `.agents/` instead.

## Adding content

- **Rules** — `.agents/rules/*.mdc` with YAML frontmatter (`description`, `alwaysApply`, optional `globs`)
- **Skills** — `.agents/skills/<name>/SKILL.md` with `name` and `description` frontmatter
- **Sub-agents** — `.agents/agents/<name>.md` with frontmatter and system prompt body

After adding a rule, skill, or agent, create matching symlinks under `.cursor/` and `.claude/` if editor support is needed:

```bash
ln -sf ../../.agents/skills/my-skill .cursor/skills/my-skill
ln -sf ../../.agents/skills/my-skill .claude/skills/my-skill
ln -sf ../../.agents/agents/my-agent.md .claude/agents/my-agent.md
```
