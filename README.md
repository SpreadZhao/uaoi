# uaoi — Use Agent On IM

Run the official Codex terminal with a second control surface in Telegram:

```sh
cd /path/to/project
uaoi codex
uaoi codex --model MODEL
uaoi codex resume
uaoi codex resume NATIVE_SESSION_ID
uaoi codex -C /path/to/other
```

uaoi requires Codex CLI 0.166.0 or later with remote TUI, Unix socket app-server,
queue and history APIs. Integration is tested against 0.166.0. It uses the official
remote terminal interface; transport and protocol compatibility may change.

## Setup

1. Create a bot with Telegram's BotFather and export its token as
   `UAOI_TELEGRAM_TOKEN`. Keep it out of configuration files and Git.
2. Create a forum supergroup, enable Topics, and add the bot as an administrator
   with Manage Topics permission. Disable bot privacy with BotFather's
   `/setprivacy` if the bot cannot receive ordinary topic messages. Remove any
   pre-existing bot webhook before using polling.
3. Copy `config.example.toml` to `~/.config/uaoi/config.toml`. Set the group's
   negative numeric ID, your numeric Telegram user ID, launcher and workspace
   paths. The bot only accepts allowlisted users in that group.
4. Run `uaoi check`, then `uaoi codex` inside a configured project.

Use `uaoi --config /absolute/path/config.toml codex ...` for a custom configuration.
The effective cwd (including `-C`/`--cd`) selects the longest matching configured
workspace. Unmatched directories require a project entry. Project configuration
selects the launcher; Codex owns model, sandbox, approvals and native settings.
All subsequent Codex arguments retain their original argument boundaries. Native
help, version and non-interactive commands run directly without starting a broker.
`--remote` is reserved for uaoi. Approval and sandbox launch options (including
`-c approval_policy=...` and `-c sandbox_mode=...`) are applied by the Codex
adapter to native start/resume/fork requests. This preserves permission choices
without triggering remote TUI's rejection of permission flags during resume;
hidden metadata threads keep their own restricted permissions.

## Lifecycle

The first interactive wrapper starts a detached local broker automatically.
Concurrent wrappers share that broker, while each TUI owns a separate app-server.
The broker closes after the last wrapper exits. Changing configuration requires
closing existing wrappers first. Child runtimes inherit their wrapper's environment
plus configured agent overrides; the broker's Telegram connection uses the first
wrapper's environment. Export proxy settings there when needed.

Each topic follows its native session's title, including automatic title generation
and `/rename`. Opening, resuming, forking or reconnecting reads the current name
again; a late rename of a previously opened session updates that session's own
topic too. Until named, a topic uses `[project] native-id`. Named topics use the
session title directly, truncated to Telegram's 128-character limit. This is
one-way synchronization from Codex to Telegram. Pending renames survive broker
restarts, retry on API failures and converge to the latest title without creating
chat messages or changing session bindings.

Each currently open session gets a topic in the one configured group. Native
`/new`, `/resume` and `/fork` switch the topic receiving events and control.
Successful navigation releases the previous Telegram control relationship; failed
navigation keeps it. A session cannot be opened by two uaoi terminals at once.
Cross-project resume requires a workspace configured with the same launcher;
use another wrapper for a different launcher/profile.

Closing a TUI makes its topic offline and stops its runtime. Old topics retain
history. Reopening a saved native session reuses its topic. uaoi does not keep
sessions alive without a CLI, restore running processes, replay uncertain work,
or execute messages sent while offline. Telegram has second-resolution message
timestamps: messages from the activation second are conservatively rejected;
send again after the online notice. Expired approval buttons cannot act on a
new connection. Starting the broker discards the bot's offline update backlog.

Run as many `uaoi codex` terminals as needed, including inside tmux. tmux detach
leaves the TUI running and Telegram connected; tmux is not a dependency. Concurrent
sessions in the same directory can edit the same files; uaoi does not create
worktrees automatically.

## Telegram controls

| Input | Behavior |
| --- | --- |
| Ordinary text | Submit to the current native session's queue |
| `/status` | Show connection status and unresolved input count |
| `/history [offset]` | Show 20 recent history records |
| `/stop` | Interrupt the current turn; native queued messages are retained |
| `/steer MESSAGE` | Steer the active turn |
| Approval buttons | Answer a pending approval |
| `/answer ID ANSWER` | Answer a question; use a JSON object for multiple questions |

The CLI and Telegram control the same running session. Commands, completions and
approvals resolved in the terminal are reflected in Telegram. Only text input is
supported. Long output is split at nearby newlines within Telegram's UTF-16 limits.

## Diagnostics and state

```sh
uaoi sessions
uaoi recovery
```

These inspect an already running broker. Logs are in the configured state directory
as `service.log`; SQLite records topic associations, history, deduplication and
outgoing delivery receipts. Known rejections are distinct from uncertain transport
outcomes. No uncertain submission is retried automatically. A send without a
platform receipt is not blindly resent. Topic creation with an uncertain outcome
requires inspecting `recovery` before manually clearing its operation record.

Local directories and sockets are private to the user. Credentials and native
history stay with the configured launcher. Old uaoi configurations, databases,
`serve`, `attach`, `import`, group binding and Telegram session creation are removed.
There is no migration or compatibility layer: stop old uaoi processes and remove
old uaoi state before using this version. Do not remove your native Codex home.

## Nix and development

```sh
nix develop
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
nix flake check
```

The Home Manager module installs the wrapper and optionally generates its settings;
it does not install a persistent user service:

```nix
programs.uaoi = {
  enable = true;
  settings = {
    im.telegram = {
      token_env = "UAOI_TELEGRAM_TOKEN";
      group = -1001234567890;
      allowed_users = [ 123456789 ];
    };
    agents.work = { kind = "codex"; command = [ "/absolute/path/to/codex" ]; };
    projects.demo = { workspace = "/path/to/project"; agent = "work"; };
  };
};
```

Native integration tests use an isolated Codex home and a loopback mock model;
they do not use account credentials:

```sh
UAOI_TEST_CODEX=/absolute/path/to/codex \
UAOI_TEST_PYTHON="$(command -v python3)" \
  cargo test -- --ignored --nocapture
```

The core owns logical sessions and delivery. Native protocols stay in the agent
adapter; Telegram identifiers and API details stay in the IM adapter. Development
instructions live in `flake.nix`; NAW generates workspace instruction files.

To include real topic-title projection in the isolated native integration test,
set `UAOI_TEST_TELEGRAM_CONFIG` to a dedicated test configuration and export its
bot token environment variable. This creates and renames one test topic without
polling the bot or interrupting existing CLI connections. `UAOI_TEST_TITLE_RECEIPT`
can name an external JSON file to record the created topic for phone inspection.
