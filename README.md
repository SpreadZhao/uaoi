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
2. Copy `config.example.toml` to `~/.config/uaoi/config.toml`, replacing paths,
   group IDs, and allowed user IDs. Use version 3 and a **fresh state directory**.
   Older configuration and state are rejected without migration. Native Codex
   history and existing Telegram topics are not deleted.
3. Configure a launcher for each profile. Agents need distinct Bots and distinct
   native profiles/homes. Store Bot credentials in external environment variables;
   put only their variable names in the configuration. A Bot cannot represent
   multiple agents, even when its token is referenced by different variables.
4. Restart uaoi after configuration changes. Configuration is not hot-reloaded.

```sh
uaoi check
uaoi start
uaoi --agent work codex -C /absolute/registered/project
uaoi --agent personal codex -C /another/registered/project
uaoi bindings
uaoi status
uaoi stop
```

`--agent NAME` is required for every `uaoi codex` invocation. uaoi never infers a
profile from cwd. `--config /absolute/path/config.toml` selects a configuration;
these options precede `codex`. Select the native profile in its configured
launcher, not with `codex -p`.

Before creating a session, uaoi canonicalizes cwd and requires an exact project
match and membership in that project's `agents`. Subdirectories are not implicit
projects. This validation also covers new sessions and forks created inside the
CLI. An unknown directory requires a configuration edit and restart. uaoi never
changes Codex's project trust or sandbox settings.

`start` and the interactive wrapper start the service on demand. `serve` runs it
in the foreground. Closing or switching a CLI leaves native tasks running; the
Bot can still control bound sessions. `stop` refuses active work, including
unbound sessions on its servers; `stop --force` interrupts it. Wait for shutdown
before restarting. Native help, version, noninteractive `exec`/`review`, and
administrative commands run directly and do not create managed conversations.
The wrapper owns `--remote` and rejects `--no-daemon`.

## Binding lifecycle

New CLI sessions get a topic only when their first task starts. Empty drafts do
not create topics. Telegram `/new` creates the session and topic immediately in
the target project's group, then returns a title link to the source topic.

Opening existing unbound native history does not import it. There is no
background discovery or project database. A session is either bound or unbound.
Once bound, its existing address determines routing, even if its cwd changes.
Changing a session's project is an explicit binding edit, never an automatic move.

`uaoi bindings` opens a snapshot in `$VISUAL`, `$EDITOR`, or `vi`. Save and exit to
apply; leave it unchanged to cancel. The snapshot covers all configured agents
and projects:

```toml
version = 2
revision = 12

[[bindings]]
project = "uaoi"
agent = "work"
session = "native-codex-session-id"
topic = "123"
```

Keep version and revision unchanged. Add an entry to bind existing native history.
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
| `/agents [TEXT\|running]` | Query all agents' bound sessions in this project group; titles link to topics |
| `/new /absolute/directory` | Create a session in an exact configured project |
| `/new@BotUsername /absolute/directory` | Explicitly select the agent creating the session |
| `/status` | Show project, agent, native ID, cwd, mode, and state |
| `/plan` | Toggle Plan and Default mode for subsequent turns |
| `/plan TASK` | Start an idle session's next task in Plan mode |
| `/default [TASK]` | Select Default mode, optionally starting a task when idle |
| `/stop` | Interrupt the current topic's task |
| `/history [OFFSET]` | Read 20 history records, starting with the most recent |
| `/help` | Show commands and their categories |

Ordinary text and task commands go only to the topic's owning Bot. Ordinary text
starts work when idle and steers an active turn. An unbound topic never selects a
task agent implicitly: use `/new@BotUsername /absolute/directory`, even if the
project currently has only one agent. In a bound topic, unqualified `/new` uses
that topic's agent. The target project must make the selected agent available.

Unqualified public queries (`/agents`, `/help`) are answered once by the first
agent in the project's `agents` list, including in another agent's work topic.
This ordering selects only the public responder, not a default task agent.
A qualified command is handled only by the addressed Bot; task commands cannot
control a different agent's topic. Bots ignore each other's messages.

There is no `/list`, `/resume`, `/steer`, `/output`, or pagination command. Use
links to navigate and the configuration file to change output detail.

## Interaction

Questions use one editable card. Numbered tabs switch questions without losing
answers. Select options or manually **Reply** to the card to replace the currently
displayed answer, including questions that offer preset choices. The reply UI
never opens automatically. Review and choose **Submit answers**, including for a
single question. Allowlisted users share its current tab and draft. Sensitive
questions must be answered in the CLI. Submission or native resolution updates
the same card and removes controls.

Plan proposals offer **Execute plan** and **Stay in Plan mode**. Execution starts
a Default-mode turn; staying starts no task. Mode-only commands do not interrupt
active work. Commands with task text require an idle session.

Answers and request state synchronize between CLI and Telegram. Native CLI
widgets have their own limitations: remote answers may close a question without
showing its answer, asynchronous editors may stay visible, and a Plan popup may
remain after a Telegram choice. These are separate native-UI acceptance checks;
uaoi does not patch those widgets.

`output = "compact"` retains concise progress, questions, plans, final answers,
and short activity details. `verbose` also includes available reasoning summaries
and full tool details. CLI user messages are marked **You · CLI**; Telegram input
is not echoed back. Routine online, offline, and successful completion events are
silent. Temporary typing indicators show activity; failures remain visible.

## Reliability and development

Uncertain submissions are not replayed automatically. Old Bot updates accumulated
while offline are discarded, and interaction controls expire on service restart.
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

Tests cover shared servers across projects, multiple Bots per group, exact path
validation, lazy topic creation, explicit binding changes, and the interaction
flows. Native tests use isolated homes and a loopback model without account
credentials. Telegram tests use a mock API. Real Bot acceptance comes last,
against manually prepared project groups.

The Home Manager module can install and run uaoi:

```nix
programs.uaoi = {
  enable = true;
  service.enable = true;
  service.environmentFile = "/home/you/.config/uaoi/secrets.env";
  settings = {
    version = 3;
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
        allowed_users = [ 123456789 ];
      };
    };
  };
};
```

Store development instructions in `flake.nix`; NAW generates `AGENTS.md`.
