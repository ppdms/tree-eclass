# Discord course-message module

Discord export, archive ingestion, retrieval, and MCP access live together in
`app/messages`. The archive is append-only source data; its SQLite search index
is rebuildable and remains separate from the official eClass document index.

## Configure

Discord support is optional. With an existing archive, open **Settings →
Discord Course Mapping** and select the real eClass course beside each real
Discord channel name. The mapping is stored transactionally in `eclass.db`; no
mapping file or environment override exists. Saving wakes both message workers
immediately.

To index an existing compatible archive, set `DISCORD_ARCHIVE_DIR` to it. In
Compose the default is `/data/discord_exports`, inside the existing persistent
`./data` mount. No sibling repository or host-specific bind mount is required.
Before the first restart, stop any process writing an older archive and move it
into place without altering its internal paths:

```console
mv /path/to/old/Exports data/discord_exports
```

To let eClass create and refresh the archive, configure **Settings → Discord
Exporter**. The form controls enabled state, token, interval, thread scope,
media downloads, and parallelism. The token is stored in plaintext in the local
`eclass.db`, injected only into the exporter child-process environment, and is
not written to runtime configuration, archive state, manifests, commands, or
logs. The exporter is pinned and installed at image-build time; its license is
recorded in `THIRD_PARTY_NOTICES.md`.

## Operation

The web process starts two independent message-module workers when configured:

- the export worker creates immutable, hash-addressed JSON segments and keeps a
  checkpoint for each real channel and thread;
- the index worker verifies complete segment manifests and updates the separate
  `/data/discord_knowledge.db` search database.

When enabled, the exporter runs immediately at startup and then at the interval
stored in Settings. Useful one-shot commands are:

```console
python -m app.messages.export_worker --once
python -m app.messages.worker --once
python -m app.messages.worker --status
python -m app.messages.exporter --config /data/discord-export.json verify
```

Incremental exports capture new messages. They do not revisit older messages to
detect later edits, deletions, reactions, or attachment changes.

## Retrieval and trust

Messages are grouped into bounded, non-overlapping conversation windows.
Search combines normalized Greek/English full-text search with the configured
embedding provider and applies query-time freshness weighting.

The MCP exposes `search_course_messages`, `read_course_messages`,
`search_course_knowledge`, and `get_message_index_status`. Discord results are
always labeled `community_discussion`; they do not override official course
material, and policy claims should be corroborated.
