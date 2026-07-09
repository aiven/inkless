# AI Agents Configuration

Shared configuration directory for AI coding assistants working in this
repository.

This directory provides a unified structure for configuring multiple AI coding
tools (OpenCode, Cursor, VS Code Copilot, Claude Code) with consistent agents,
skills, and commands.

## How It Works

The main entry point for all AI tools is [`AGENTS.md`](../AGENTS.md) in the
repository root. Each tool-specific directory (`.opencode/`, `.cursor/`,
`.claude/`) symlinks its `agents/`, `commands/`, and `skills/` back to this
shared `.ai-agents/` directory, ensuring consistent configuration across tools.
Tools that read a single instructions file instead of a directory (VS Code
Copilot via `.github/copilot-instructions.md`) point at `AGENTS.md` directly.

## Directory Structure

### `agents/`

Custom agent definitions that configure AI behavior for specific development
roles.

Agents allow you to create specialized personas (e.g., security reviewer,
planner, code reviewer) with their own instructions, available tools, and
behavior.

**Tool Documentation:**

- [OpenCode Agents](https://opencode.ai/docs/agents/)
- [Cursor Rules for AI](https://docs.cursor.com/context/rules-for-ai)
- [VS Code Copilot Custom Agents](https://code.visualstudio.com/docs/copilot/customization/custom-agents)
- [Claude Code Memory Management](https://docs.anthropic.com/en/docs/claude-code/memory)

### `commands/`

Reusable prompt commands (also called "prompt files") that can be triggered with
a `/` prefix in chat.

Commands help standardize workflows and make common tasks more efficient (e.g.,
`/review-code`, `/write-tests`, `/create-pr`).

**Tool Documentation:**

- [OpenCode Commands](https://opencode.ai/docs/commands/)
- [Cursor Commands](https://docs.cursor.com/context/commands)
- [VS Code Copilot Prompt Files](https://code.visualstudio.com/docs/copilot/customization/prompt-files)
- [Claude Code Custom Slash Commands](https://docs.anthropic.com/en/docs/claude-code/best-practices#create-custom-slash-commands)

### `rules/`

Small rule files referenced from [`AGENTS.md`](../AGENTS.md) (for example as
**MUST**-follow defaults).

### `skills/`

Skill definitions for specialized tasks. Skills are folders of instructions,
scripts, and resources that AI assistants can load when relevant.

Unlike agents that define personas, skills enable specialized capabilities and
workflows that can include scripts, examples, and other resources.

**Tool Documentation:**

- [OpenCode Agent Skills](https://opencode.ai/docs/skills/)
- [Cursor Skills](https://docs.cursor.com/context/skills)
- [VS Code Copilot Agent Skills](https://code.visualstudio.com/docs/copilot/customization/agent-skills)
- [Claude Code Skills](https://docs.anthropic.com/en/docs/claude-code/skills#extend-claude-with-skills)
- [Agent Skills Standard](https://agentskills.io)

## Tool Integration

| Tool            | Config Directory | Main Instructions                 |
| --------------- | ---------------- | --------------------------------- |
| OpenCode        | `.opencode/`     | `AGENTS.md` via `opencode.json`   |
| Cursor          | `.cursor/`       | `AGENTS.md` (auto-discovered)     |
| VS Code Copilot | `.github/`       | `.github/copilot-instructions.md` |
| Claude Code     | `.claude/`       | `CLAUDE.md` → `AGENTS.md`         |

## Adding Content

When adding new agents, commands, or skills, place them in the appropriate
subdirectory here. The symlinks in each tool's directory will automatically
make them available.

For tool-specific customizations that don't apply to other tools, place them
directly in that tool's directory (e.g., `.cursor/rules/` for Cursor-only
rules).
