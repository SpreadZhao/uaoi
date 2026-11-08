# uaoi — Use Agent On IM

Control the same Codex sessions from the official terminal and Telegram.

- A **project** declares one working directory, one Telegram forum group, and its
  available agents. Projects exist only in the configuration file.
- An **agent** owns one Bot, one native profile, and one shared app-server. The
  same agent/server can serve several projects; a project can use several agents.
- A **session** belongs to one agent and is bound to one topic. The group/topic
  address is independent of the Bot that sends and receives its messages.

Requires Codex CLI 0.166.0 or newer. Native integration tests cover 0.167.0.

## Setup

1. Manually create a Telegram group for each project and enable Topics. Add the
   project's Bots and grant each Manage Topics permission. Disable Bot privacy
   so each Bot receives ordinary topic messages. Remove existing webhooks before
   using long polling. uaoi never creates groups or invites Bots.
2. Copy `config.example.toml` to `~/.config/uaoi/config.toml`, replacing paths
   and group IDs. Use version 4 and a **fresh state directory**.
   Older configuration and state are rejected without migration. Native Codex
   history and existing Telegram topics are not deleted.
3. Configure a launcher for each profile. Agents need distinct Bots and distinct
   native profiles/homes. Set exactly one of `telegram.token` (a literal token)
   or `telegram.token_env` (an environment variable name) for each agent. A Bot
   cannot represent multiple agents, even through different credential sources.
   Keep token-bearing configuration outside Git with file permissions `0600`.
4. All members of a configured project group, including administrators sending
   anonymously as that group, can control its sessions, answer questions, and
   use the shared command menu. Private chats, other groups, channel posts, and
   messages from other Bots are ignored. There is no user allowlist.
5. Restart uaoi after configuration changes. Configuration is not hot-reloaded.

For a token stored directly in your local TOML configuration:

```toml
[agents.supazodiac.telegram]
token = "YOUR_SUPA_CODEX_BOT_TOKEN"
```

Remove `token_env` from that section. This mode needs no token environment
variable. With `token_env = "UAOI_BOT_SUPA_CODEX"` instead, export that variable
in the launching shell. Configuration changes require closing all uaoi terminals.

```sh
uaoi check
uaoi --agent work -- -C /absolute/registered/project
uaoi --agent personal -- -C /another/registered/project
uaoi bindings
uaoi status
```

`uaoi --agent NAME` launches the configured agent. `--agent NAME` is required;
uaoi never infers a profile from cwd. `--config /absolute/path/config.toml`
selects a configuration. uaoi options precede `--`, and arguments after `--` go
unchanged to the native CLI. Select the native profile in its configured
launcher, not with `codex -p`.

Before creating a session, uaoi canonicalizes cwd and requires an exact project
match and membership in that project's `agents`. Subdirectories are not implicit
projects. This validation also covers new sessions and forks created inside the
CLI. An unknown directory requires a configuration edit and restart. uaoi never
changes Codex's project trust or sandbox settings.

Each interactive `uaoi --agent NAME` invocation is one **UAOI instance**. The
first instance starts the shared broker and that agent's Bot and app-server.
Further instances of the same agent share them. Switching native sessions or
projects does not create another instance. Closing one instance releases its
connection; the other instances and their Telegram sessions continue. Closing
an agent's **last instance** interrupts all its work (including Telegram-created
and unbound sessions), stops its Bot and app-server, and reclaims their processes
and sockets. Other agents are unaffected. When no instances remain, the broker
exits too. No persistent service or systemd unit is used.

An agent's first instance installs its project-group command menus; its last
instance clears them. On startup, uaoi clears stale menus for configured Bots
whose credentials are available, without starting their agents. These dedicated
Bots' generic command menus are cleared too, so an offline Bot cannot fall back
to an old default menu. A forced process kill or Telegram/network failure can
leave menus visible until the next successful synchronization. Cleanup is
bounded and never keeps a native runtime alive.

Each newly activated agent uses its first instance's environment plus the agent's
configured environment. Inactive agents do not need credentials. `status`,
`bindings`, and `recovery` never activate agents or keep uaoi running; open a
terminal before editing bindings. A configuration change requires closing all
uaoi terminals and then launching again. `status` reports each agent's instance
count and runtime generation. The existing JSON field `clients` counts UAOI
instances, not native sessions.

Native help, version, noninteractive `exec`/`review`, and administrative commands
run directly without a managed runtime. For example, `uaoi --agent work -- --help`
shows native help; `uaoi --help` shows uaoi help. The wrapper owns `--remote` and
rejects `--no-daemon`. There are no `codex`, `start`, `serve`, or `stop` subcommands.

## Binding lifecycle

A session that has never been bound gets a topic when uaoi observes a new user
or assistant message in a configured project. This applies equally to new CLI
sessions and resumed native history. The topic binds to the original session,
so Telegram continues the same conversation and context. Opening, renaming,
changing modes, or reading old messages does not trigger binding. Empty drafts
do not create topics. Telegram `/new` explicitly creates a session and binds its
topic immediately in the current group's configured project directory.

There is no background discovery or bulk import. Once bound, the existing
address determines routing, even if the session's cwd changes. Changing a
session's project is an explicit binding edit, never an automatic move. Binding
history is retained after unbinding, so later messages do not bind it again.

`uaoi bindings` opens a snapshot in `$VISUAL`, `$EDITOR`, or `vi`. Save and exit to
apply; leave it unchanged to cancel. The snapshot covers all configured agents
and projects, including inactive ones:

```toml
version = 3
revision = 12

[[bindings]]
project = "uaoi"
agent = "work"
session = "native-codex-session-id"
topic = "123"
```

Keep version and revision unchanged. Unchanged bindings for inactive agents are
preserved; adding or changing one requires an open terminal for that agent. Add
an entry to bind existing native history.
Omit `topic` to allocate one in the specified project, or provide an existing
positive topic ID. Removing an entry unbinds it without deleting history, deleting
the topic, or interrupting native work. Later activity does not rebind it. Old
buttons and replies cannot control an unbound or reassigned session.

The broker validates project membership, all native IDs, and unique session/topic
ownership before allocating topics. It applies the mapping transactionally and
rejects concurrent edits. Failed edits retain a local copy. After an uncertain
result, reopen the snapshot and inspect `uaoi recovery` before retrying. Runtime
receipts and bindings live in private SQLite state; do not edit its database files.
If configuration removes a bound group or agent membership, restore that
configuration to edit the affected bindings explicitly.

## Telegram commands and routing

All topics expose the same menu. Descriptions distinguish **Manage** and **Task**;
there is no privileged management topic.

| Command | Behavior |
| --- | --- |
| `/agents [TEXT\|running]` | List this project's bound sessions; titles link to topics. Optionally search or show working sessions |
| `/new` | Create a session and topic using this group's project directory |
| `/new@BotUsername` | Explicitly select the agent creating the session in this project |
| `/status` | In an unbound topic, show the project directory, bound-session count, and available agents; in a session topic, show that session's status |
| `/plan` | Toggle Plan and Default mode for subsequent turns in this session |
| `/plan TASK` | Start this idle session's next task in Plan mode |
| `/default [TASK]` | Select Default mode, optionally starting a task in this idle session |
| `/stop` | Interrupt only this topic's current task |
| `/history [OFFSET]` | Read 20 history records for this session, starting with the most recent |
| `/help` | Show this project's commands and their scope |

Project commands work in General and existing session topics. `/new` takes no
arguments: the group determines the project and directory. In General or any
unbound topic, a single online agent is selected automatically; when several
agents are online, use `/new@BotUsername` to choose. In a bound topic, unqualified
`/new` uses that topic's agent. Creating sessions in another project requires
sending `/new` in that project's group.

`/agents` queries only sessions already bound to topics in this group. It does
not scan other projects or discover/import unbound native history. Offline agents'
bound sessions remain listed as unavailable.

Ordinary text and session commands go only to the topic's owning Bot. Ordinary
text starts work when idle and steers an active turn. In General, use `/new` or
`/agents` and follow the resulting topic link before sending a task.

Unqualified public queries (`/agents`, `/help`) are answered once by the first
active agent in the project's `agents` list, including in another agent's work
topic. `/status` in General uses the same responder; in a bound topic it uses the
session owner. A qualified command is handled only by the addressed Bot; session
commands cannot control a different agent's topic. Bots ignore each other's
messages. `/new`, `/status`, `/stop`, and `/help` reject extra arguments.

There is no `/list`, `/resume`, `/steer`, `/output`, or pagination command. Use
links to navigate and the configuration file to change output detail.

## Interaction

Questions use one editable card. Numbered tabs switch questions without losing
answers. Select options or manually **Reply** to the card to replace the currently
displayed answer, including questions that offer preset choices. The reply UI
never opens automatically. Review and choose **Submit answers**, including for a
single question. Group members share its current tab and draft. Sensitive
questions must be answered in the CLI. Submission or native resolution updates
the same card and removes controls.

Plan proposals offer **Execute plan** and **Stay in Plan mode**. Execution starts
a Default-mode turn; staying starts no task. Mode-only commands do not interrupt
active work. Commands with task text require an idle session.

Answers and request state synchronize between CLI and Telegram. Asynchronous
answers retain the native question identities, so once Codex processes options
or custom text from Telegram, it closes the corresponding CLI editors and
displays the answers in the transcript. During active work, answers may first
queue until the runtime processes pending input. Native CLI widgets still have
their own limitations: remote answers to blocking questions may close the prompt without showing an
answer summary, and a Plan popup may remain after a Telegram choice. These are
separate native-UI acceptance checks; uaoi does not patch those widgets.

Each agent's `output` setting controls work messages only:

| Value | Work messages shown in Telegram |
| --- | --- |
| `full` | All available progress, reasoning summaries, individual tool details, plans, and replies |
| `compact` (default) | Progress and replies, with consecutive tool calls between visible messages updating one short activity message |
| `minimal` | User messages and the last assistant reply from each task, published when the task ends |

In compact mode, a new text message starts a new tool group, preserving the
earlier activity message. Streaming updates to the same text do not start more
groups. Minimal mode suppresses intermediate text, plans, reasoning, and normal
tool output; task failures remain visible. `full` replaces the former `verbose`
configuration value.

Questions, approvals, plan confirmations, their answer/status updates, and
explicit command replies work the same in all three modes. CLI user messages
are marked **You · CLI**; Telegram input is not echoed back. The setting does not
change native CLI output or what the agent executes. Routine online, offline,
and successful completion notices are silent. Temporary typing indicators show
activity without adding messages.

## Reliability and development

The broker owns UAOI instance lifetimes. Agent adapters own shared native
runtimes and their per-thread state; IM adapters own platform addresses and
protocols. Within the service, `output` handles message projection and delivery
payloads, while `interaction` handles question drafts and cards. Codex's
`gateway` forwards native terminal connections and validates session creation.
These are internal module boundaries; they do not add processes or change
configuration, binding, or delivery formats.

Uncertain submissions are not replayed automatically. Old Bot updates accumulated
while offline are discarded on agent activation, and interaction controls expire
when their agent stops. Bindings and recorded history survive normal exits.
Bound native sessions attach again; interrupted tasks are not replayed. Concurrent
sessions can edit the same files; uaoi does not create worktrees.

```sh
nix develop
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
nix flake check
UAOI_TEST_CODEX=/absolute/path/to/codex cargo test -- --ignored --nocapture
```

Tests cover terminal lifetimes, independent profile activation, startup rollback,
shared servers across projects, multiple Bots per group, exact path
validation, lazy topic creation, explicit binding changes, and the interaction
flows. Native tests use isolated homes and a loopback model without account
credentials. Telegram tests use a mock API. Real Bot acceptance comes last,
against manually prepared project groups.

The Home Manager module installs uaoi and writes its configuration; launch it
from a terminal:

```nix
programs.uaoi = {
  enable = true;
  settings = {
    version = 4;
    agents.work = {
      kind = "codex";
      command = [ "/absolute/path/to/codex-work" ];
      output = "compact";
      telegram.token_env = "UAOI_WORK_BOT_TOKEN";
    };
    projects.example = {
      cwd = "/home/you/workspaces/example";
      agents = [ "work" ];
      telegram = {
        group = -1001234567890;
      };
    };
  };
};
```

Store development instructions in `flake.nix`; NAW generates `AGENTS.md`.
