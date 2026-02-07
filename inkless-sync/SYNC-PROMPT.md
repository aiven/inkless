# Inkless Sync Prompt

Copy and paste this prompt to start a sync session:

---

```
I need to sync inkless with upstream Apache Kafka.

## Available Sync Types

1. **Main Sync** - Sync the `main` branch with Apache Kafka trunk
   - Use for: Weekly/biweekly development sync
   - Prompt: `inkless-sync/MAIN-SYNC-PROMPT.md`

2. **Release Sync** - Sync a release branch (e.g., `inkless-4.0`) with upstream patches
   - Use for: When Apache releases a patch (e.g., 4.0.1)
   - Prompt: `inkless-sync/RELEASE-SYNC-PROMPT.md`

## Context

- Scripts are in `inkless-sync/`
- Session templates track progress in `.sync/`
- Action plans have file-by-file playbooks

Please help me determine which sync type I need and guide me through the process.
Read the appropriate prompt file for detailed instructions.
```

---

The agent will:
1. Ask which sync type you need (or infer from context)
2. Read the appropriate detailed prompt
3. Guide you through the sync process
