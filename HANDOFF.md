# Project routing and shared Codex runtime handoff

Updated: 2026-11-07

## Status

Implementation and local acceptance are complete. Live Telegram acceptance was
explicitly deferred by the user. The existing running service was not restarted,
and no live group, topic, Bot credential, or native history was changed by this
implementation pass.

This is a breaking release relative to the initial wrapper and earlier in-progress
configurations. Configuration version is **3**, the SQLite schema version is **5**,
and the editable binding document version is **2**. Older formats are rejected;
there is no migration or automatic import. Use a new state directory.

## Model and routing

- A project exists only in configuration: canonical cwd, unique Telegram group,
  allowed users, and an ordered list of available agents.
- An agent owns a distinct Bot, native profile/launcher, and shared app-server.
  Its server can serve multiple projects. Bot IDs are checked at startup so two
  agents cannot accidentally poll the same Bot through different token variables.
- A session is identified by agent and native thread ID. A topic has exactly one
  session and therefore one responsible agent. Group/topic addresses are stored
  independently of Bot identity; outbound receipts record their sending agent.
- Ordinary messages and task commands go to the topic owner. Unqualified public
  queries (`/agents`, `/help`) go to the project's first agent, which is not a
  default task executor. Explicit `@BotUsername` commands select that Bot.
- `/new` in an unbound topic requires `@BotUsername`. In a bound topic it can use
  the current owner. A cross-project creation returns a link in the source topic
  and creates the new topic in the target project's group.

Creation canonicalizes cwd and requires an exact configured project and agent
membership. The gateway also validates CLI-internal creation, inherited fork
paths, and the first turn of a new CLI draft. Empty CLI drafts do not create
Telegram topics. Opening unbound native history does not import it.

Existing bindings govern delivery even after cwd changes. `uaoi bindings` edits
all bindings, including their explicit project assignment. It validates native
IDs before allocating topics, rejects stale revisions and duplicate ownership,
and applies the mapping transactionally. Unbinding cancels pending delivery and
invalidates controls without deleting the native session or interrupting work.
There is no background session discovery and no project database.

## Preserved interactions

- Ask-user questions use one editable card with question navigation, shared
  drafts, choice buttons, manual Reply for custom answers, review, and explicit
  submission. CLI answers close or update the same card.
- Plan mode supports `/plan` toggle, inline planning/default tasks, and the
  Execute / Stay in Plan choice. Ordinary messages steer active work.
- Compact output is the default; detail is configurable per agent only.
  CLI-origin input is labeled `You · CLI`. Routine lifecycle notifications stay
  silent, with temporary typing indicators for active work.
- CLI navigation and exit do not terminate active shared-server tasks. Service
  stop checks native active work, including deliberately unbound sessions.
- Removed commands stay removed: `/list`, `/resume`, `/steer`, `/output`, and
  pagination. All topics share the categorized command menu.

The official CLI keeps some interaction state locally. A remote answer can close
its question without displaying the answer; an asynchronous editor or Plan popup
can remain visible. Native widget behavior needs its own acceptance check and is
not patched by uaoi.

## Code map

- `src/config.rs`: versioned configuration, canonical project paths, memberships.
- `src/im/telegram.rs`: Bot identity, command addressing, shared topic addresses,
  Telegram entities, cards, polling, and delivery.
- `src/service.rs`: owner/public routing, topic creation, binding edits, commands,
  question drafts, compact output, and outbox workers.
- `src/agent/codex.rs`: shared native runtimes, gateway creation validation,
  deferred draft binding, mode/answer synchronization, and automatic steer.
- `src/store.rs`: schema rejection, unique bindings, revisions, receipts, controls.
- `src/bindings.rs`: local editor workflow and retained failed edit documents.
- `src/project_tests.rs`, `src/tests.rs`, `src/native_tests.rs`: acceptance coverage.
- `config.example.toml` and `README.md`: current configuration and operating guide.

## Validation

- `cargo fmt --check`: passed.
- `cargo clippy --all-targets -- -D warnings`: passed.
- `cargo test`: 27 tests passed; the external native test is ignored by default.
- External native acceptance: passed with Codex 0.167.0, isolated native homes,
  and a loopback model; no account credentials or live Telegram API were used.
- `nix flake check`: passed on x86_64-linux; aarch64-linux was not built.

Coverage includes two projects sharing a server, two Bots receiving the same
message with one execution, primary and addressed queries, cross-group creation,
unknown directories, internal CLI creation/fork validation, empty drafts, unbound
history, binding reassignment/restart, expired controls after unbinding, and all
existing question/Plan/steer cases.

## Next deployment and live acceptance

1. Prepare configuration version 3 from `config.example.toml`. Use a fresh state
   directory. Keep Bot tokens only in an external environment file.
2. Manually prepare two forum groups and two distinct Bots. Add both Bots to one
   group, and make the shared agent available in the other project. Grant Manage
   Topics and disable Bot privacy as needed for ordinary messages.
3. Validate with `uaoi --config /absolute/config.toml check`. Once current work is
   finished, stop the previous service, wait for it to exit, export the external
   credentials, and start the new binary with the new configuration. There is no
   hot reload. No NixOS or Home Manager switch is required for local acceptance.
4. Launch with `uaoi --config /absolute/config.toml --agent work codex -C /exact/project`.
   Use the same configuration with `uaoi bindings` to bind selected existing
   native sessions explicitly; old uaoi state is not imported.
5. In the live groups, verify one response/execution per message, Bot ownership,
   cross-group `/new@BotUsername`, clickable `/agents` titles, CLI/Telegram
   questions and mode changes, custom replies, steer during work, and continued
   Telegram control after CLI exit. Check native widget limitations separately.

Live acceptance remains intentionally outstanding. Do not infer live verification
from the mock Telegram or loopback native test results.
