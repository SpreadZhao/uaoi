# UAOI instance lifecycle handoff

Updated: 2026-11-08

## Current implementation

This section supersedes the archived v3 deployment notes below. Version 4
removes user allowlists and ties agent runtimes to interactive instances.

- Launch with `uaoi --agent NAME`; the configured adapter determines the native
  executable. Pass native arguments after `--`, for example
  `uaoi --agent personal -- -C /absolute/project`.
- Each profile's first interactive CLI starts its Bot and shared app-server.
  Further CLIs share them. Its last CLI stops that profile, including active
  work. Other profiles continue. The shared uaoi broker exits at zero clients.
- Management commands never activate a profile or keep it running. Native help,
  version, and noninteractive commands bypass the broker. Public `codex`,
  `start`, `serve`, and `stop` commands and Home Manager service options are gone.
- Each agent accepts exactly one `telegram.token` (literal token in the local
  configuration) or `telegram.token_env` (environment reference). Literal tokens
  require no environment variable. Tests cover both credential sources, missing
  and conflicting settings, duplicate tokens, and credential-free check output.
  Do not put real credentials in the repository.
- Configuration version 4, database schema 6, binding document version 3.
  Use fresh uaoi state; older formats are rejected without migration. Normal
  exits preserve new bindings and history. Native Codex history is not deleted.
- Runtime queries cannot revive inactive profiles. Their existing bindings stay
  editable as unchanged entries; changes require an open CLI for that profile.
  Public Telegram queries select the first active project agent. Topic owners
  are never reassigned implicitly. Interaction expiry is scoped to the profile.
- `src/broker.rs` owns admission, connection counts, profile activation, and
  teardown. Startup has a wrapper-held bootstrap socket and serialized discovery.
  App-server creation is explicit in the Codex adapter. Telegram routing metadata
  is available before activation; credentials are resolved and network access
  begins only on profile activation.
- Telegram project commands are scoped to the current group. `/new` accepts no
  directory argument and uses that project's configured cwd. In an unbound
  topic it chooses the sole online project agent; with multiple online agents
  the user must address `/new@BotUsername`. In a bound topic it defaults to the
  owner. `/agents [running|text]` reads only this group's explicit bindings,
  never unbound native history or another project's runtime status.
- `/status` shows project configuration, online agents, and bound-session count
  in General/unbound topics, and the existing session status in a bound topic.
  `/help` explains project and session commands separately. `/new`, `/status`,
  `/stop`, and `/help` reject extra arguments. `/plan`, `/default`, `/stop`, and
  `/history` remain confined to the current topic's session.
- Telegram groups are the access boundary. Every member of a configured project
  group can control its sessions and answer shared questions. The user allowlist
  configuration and sender authorization checks have been removed. Private
  chats, unconfigured groups, and Bot messages remain ignored.
- Anonymous administrator messages are accepted when `sender_chat` is the
  configured receiving group, even when Telegram supplies a bot-shaped `from`
  compatibility sender. Both commands and task messages follow this rule;
  other Bots and posts from external channels remain ignored.
- Telegram command menus are registered once per project group for all members.
  An unavailable group does not prevent later project menus from being installed;
  failures log the affected project and group.
- Menus follow profile lifetime: first CLI shows them, last CLI clears them.
  Broker startup clears stale menus for all configured Bots whose credentials
  are available, without activating their native runtimes. Generic and
  administrator scopes are cleared to prevent fallback to old default menus.
  Cleanup has a five-second deadline after the native runtime stops; network
  failures or forced kills can leave stale menus until a later synchronization.
- Automatic binding is driven by a new live user/assistant message from a
  never-bound session in a configured project. New and resumed CLI sessions
  follow the same rule. Opening, renaming, mode changes, or history alone do not
  bind; Telegram `/new` still binds immediately as an explicit creation action.
  The topic uses the original native session ID and its full conversation
  context. Persistent binding receipts prevent automatic rebinding after an
  explicit unbind. Existing bindings continue to determine routing regardless
  of later cwd changes; there is no background history import.
- Original project matching, explicit bindings, question cards, and native mode
  handling remain in place.
- Async question replies preserve the original native item and question index
  and use Codex's structured question-reply envelope. Telegram options and
  custom text now dismiss the matching native editors and render as answers,
  rather than ordinary follow-up messages, once the runtime processes the input.
  Steer acknowledgments only confirm queueing, so active-work regression waits
  for the committed native user-message event before checking editor dismissal.
  Blocking question prompts still have a separate Codex 0.167.0 limitation: remote resolution closes the prompt
  without the answer summary that a local submission inserts.
- Compact tool activity is grouped between visible messages, rather than using
  one permanent message per session. Each group shows its latest tool summary;
  earlier groups stay in the timeline. Streaming text updates do not split a
  group repeatedly, and late tool results update the group where the tool began.
  User input and turn completion also separate groups.
- Work output has three configuration values: `full` (all available work
  output, replacing `verbose`), `compact` (the existing grouped activity view),
  and `minimal` (user input and the last assistant reply, buffered until task
  completion). Errors remain visible. Questions, approvals, plan decisions,
  their status updates, and explicit command replies bypass work filtering.
  Display mode does not change native CLI output or agent execution.

## Behavior-preserving simplification (2026-11-08)

- Broker and native adapter ownership now use `instance` names. The status JSON
  still exposes `clients`, with the same count and lifecycle behavior. Session
  and project switches do not change that count.
- Each native thread has one state record containing its cached session,
  subscription, starting/current turn, mode, plan, and draft directory. Drafts
  remain distinct from registered and subscribed sessions. Request correlation
  and answer deduplication keep their separate native identities.
- Native gateway code lives in `src/agent/codex/gateway.rs`. Service projection,
  grouped/minimal output state, and delivery payloads live in
  `src/service/output.rs`; question cards and drafts live in
  `src/service/interaction.rs`. These are modules on the existing runtime and
  Service, not new service layers.
- The service has two message output paths: choose the topic/project Bot or use
  an explicitly chosen Bot. Both share splitting, links, buttons, and outbox
  writes. Compact tool groups and minimal final responses share per-topic state.
- Native CLI parsing shares the value-taking option list. Binding mutations
  share history/title bookkeeping inside the existing SQLite transactions.
- The broker's global startup/teardown lock, retry deadlines, menu lifecycle,
  process cleanup, formats, and uncertain-outcome handling are unchanged.
- `terminal_fixture.py` is retained: the native async-question acceptance test
  now references it. Do not remove it as unused.
- Regression validation passed: 51 default tests plus Codex 0.167.0 native
  acceptance. The pre-refactor native scenarios also passed against a fixed
  snapshot of the refactored production code. Concurrent async-question test
  changes were preserved; the combined native test snapshot used for verification
  passed too.

## Validation

- `cargo fmt --check`: passed.
- `cargo clippy --all-targets -- -D warnings`: passed.
- `cargo test --all-targets`: 51 passed, one native integration test ignored by
  default. Includes actual wrapper subprocess termination and child cleanup.
- Native integration with Codex 0.167.0: passed separately, using isolated homes
  and a loopback model. Includes active-work teardown, an unaffected second
  profile, offline query rejection, and explicit reactivation. It also seeds a
  conversation before uaoi starts, resumes it without a topic, verifies that a
  new message binds the original ID, continues via the simulated IM with old
  context, and checks that explicit unbinding prevents rebinding. This
  continuation uses minimal output and verifies that native intermediate
  commentary is omitted while each final reply reaches the IM outbox.
  Async-question regression also covers two questions with identical titles,
  preset and Unicode custom answers, submission during active work and after
  completion, rejection of duplicate submission, visible native answer text,
  and subsequent terminal input without a stale question editor.
- `nix flake check --no-update-lock-file`: passed on x86_64-linux, including
  the package and default tests, Home Manager module, and workspace checks.
  aarch64-linux was not built on this host. Python is a test-only package input.

Native tests sent no live Telegram messages. Startup printed the existing
Codex warnings about temporary-home aliases and using its bundled bubblewrap;
these did not fail the test.

The group-access change was checked against a mock Telegram API: arbitrary
members' messages and callbacks are accepted, all members share question drafts,
and private chats, unconfigured groups, and Bot messages are ignored. Read-only
live checks confirmed the new group and both Bots; Manage Topics still needed
to be granted at the time of the check. No live messages were sent.

An inactive Bot inherited an old default command menu during live acceptance.
This prompted profile-lifetime menu synchronization, including clearing default
menus instead of merely deleting the project-group menu. Existing member-scoped
menus from the removed allowlist setup were cleared in reachable project groups;
new code installs shared group menus. Live readback confirmed empty default
menus, an empty group menu for the offline profile, and the expected project
commands for the online profile.

## Deployment

Build the package and create a version 4 configuration from `config.example.toml`
with fresh state/runtime directories. Set either a literal `telegram.token` in a
private local configuration or `telegram.token_env` with its corresponding shell
environment variable. Run `uaoi --config /absolute/config.toml --agent NAME`.
Rebuild and restart existing CLIs to apply command-menu changes. Do not reuse v3
state or run NixOS or Home Manager switch commands as part of deployment.

Keep credentials, local deployment paths, process IDs, runtime generations,
native session IDs, group/topic IDs, and current bindings outside this document.

## Manual v4 live acceptance (2026-11-08)

The user operated the terminals and Telegram. Read-only process/database checks
confirmed these results without recording local runtime identifiers:

- A configured Bot created and used a project topic after receiving Manage
  Topics permission. The earlier uncertain creation attempt was not replayed.
- Two CLIs resuming the same native session shared one runtime. Closing one
  changed the instance count from two to one and kept Telegram usable from the
  original topic without restarting the runtime.
- Closing the last CLI removed the broker, app-server, control socket, and
  runtime sockets while preserving the topic/session binding.
- A fresh launch resumed the same session in a new runtime generation and
  accepted Telegram input in the original topic.
- Two profiles ran concurrently with distinct app-servers and delivered replies
  in their own project groups. Closing one left the other's broker connection,
  app-server, runtime generation, and Telegram control working.
- An unavailable configured group exposed a command-menu registration failure.
  The fix isolates failures by scope so later groups still receive their menus.
  The project-command audit then covered group-only creation/listing and
  context-sensitive status in automated tests.

These results cover the tested two-profile setup on x86_64-linux. They do not
imply live acceptance on other platforms or with additional adapters.

---

## Archived v3 handoff (historical only)

The following notes describe the prior implementation. Its persistent-service
behavior, access rules, and launch/deployment commands are obsolete. Live v3
acceptance subsequently covered cross-group routing, shared question cards,
Plan decisions, steering, interruption, binding edits, and restart recovery.
Local deployment and runtime details are intentionally omitted.

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
