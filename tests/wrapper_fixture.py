"""Exercise the actual wrapper/broker/TUI lifecycle without account credentials."""
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import signal
import sqlite3
import struct
import subprocess
import tempfile
import termios
import threading
import time
import re
import http.server

binary = os.environ['UAOI_BINARY']
native = os.environ['UAOI_TEST_CODEX']
class Model(http.server.BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_POST(self):
        body=self.rfile.read(int(self.headers.get('Content-Length',0)))
        text=json.dumps({'title':'Process test'}) if b'Generate a concise' in body else 'PROCESS-OK'
        item={'type':'message','role':'assistant','id':'msg-test','content':[{'type':'output_text','text':text}]}
        events=[{'type':'response.created','response':{'id':'response-test'}},{'type':'response.output_item.done','item':item},{'type':'response.completed','response':{'id':'response-test','usage':{'input_tokens':1,'output_tokens':1,'total_tokens':2}}}]
        data=''.join('event: '+e['type']+'\ndata: '+json.dumps(e)+'\n\n' for e in events).encode()
        self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
model=http.server.ThreadingHTTPServer(('127.0.0.1',0),Model)
threading.Thread(target=model.serve_forever,daemon=True).start()
with tempfile.TemporaryDirectory(prefix='uaoi-wrapper-') as folder:
    root = Path(folder)
    workspace = root / 'work'
    workspace.mkdir()
    nested=workspace/'nested'
    nested.mkdir()
    native_home = root / 'codex'
    native_home.mkdir()
    (native_home / 'config.toml').write_text(f'''model = "test-default"
model_provider = "mock"
approval_policy = "never"
sandbox_mode = "read-only"
check_for_update_on_startup = false
[model_providers.mock]
name = "Unused mock"
base_url = "http://127.0.0.1:{model.server_port}/v1"
wire_api = "responses"
requires_openai_auth = false
[projects.{json.dumps(str(workspace))}]
trust_level = "trusted"
[projects.{json.dumps(str(nested))}]
trust_level = "trusted"
''')
    config = root / 'custom config.toml'
    config.write_text(f'''state_dir = {json.dumps(str(root / 'state'))}
runtime_dir = {json.dumps(str(root / 'run'))}
[agents.test]
kind = "codex"
command = [{json.dumps(native)}]
env = {{ CODEX_HOME = {json.dumps(str(native_home))}, CODEX_SQLITE_HOME = {json.dumps(str(native_home))}, OPENAI_API_KEY = "" }}
[projects.demo]
workspace = {json.dumps(str(workspace))}
agent = "test"
[projects.child]
workspace = {json.dumps(str(nested))}
agent = "test"
''')
    base_command = [binary, '--config', str(config)]
    help_text = subprocess.check_output(base_command + ['codex', '--help'], text=True)
    assert 'Codex CLI' in help_text and '--remote' in help_text, help_text
    assert not (root / 'run/control.sock').exists()
    sessions = []
    def start(*args):
        pid, terminal = pty.fork()
        if pid == 0:
            os.chdir(workspace)
            os.environ['TERM'] = 'xterm-256color'
            os.execv(binary, base_command + ['codex', *args])
        fcntl.ioctl(terminal, termios.TIOCSWINSZ, struct.pack('HHHH', 40, 120, 0, 0))
        output = bytearray()
        def pump():
            while True:
                try:
                    if not select.select([terminal], [], [], .2)[0]:
                        continue
                    data = os.read(terminal, 65536)
                    if not data:
                        return
                    output.extend(data)
                    for query, answer in [(b'\x1b[6n', b'\x1b[1;1R'), (b'\x1b[c', b'\x1b[?1;2c'), (b'\x1b]10;?', b'\x1b]10;rgb:ffff/ffff/ffff\x1b\\'), (b'\x1b]11;?', b'\x1b]11;rgb:0000/0000/0000\x1b\\')]:
                        if query in data:
                            os.write(terminal, answer)
                except OSError:
                    return
        threading.Thread(target=pump, daemon=True).start()
        sessions.append((pid, terminal, output))
        return sessions[-1]
    def wait(check, description, timeout=25):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if check():
                return
            time.sleep(.1)
        logs = [(root / 'state/service.log').read_text() if (root / 'state/service.log').exists() else 'no broker log']
        logs.extend(re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]','',bytes(s[2]).decode(errors='replace'))[-2000:] for s in sessions)
        raise AssertionError(description + '\n' + '\n'.join(logs))
    def online():
        try:
            return [s for s in json.loads(subprocess.check_output(base_command + ['sessions'], stderr=subprocess.DEVNULL)) if s['online']]
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            return []
    def send(session, text):
        os.write(session[1], text.encode())
        time.sleep(.2)
        os.write(session[1], b'\r')
    def stop(session, sig=signal.SIGTERM):
        os.kill(session[0], sig)
        end = time.monotonic()+10
        while time.monotonic()<end:
            if os.waitpid(session[0], os.WNOHANG)[0]:
                return
            time.sleep(.1)
        raise AssertionError('wrapper did not exit')
    try:
        a = start('--model', 'explicit-model', '--ask-for-approval', 'never', '--sandbox', 'read-only')
        b = start('-C', str(nested))
        wait(lambda: len(online()) == 2, 'parallel first startup did not create two sessions')
        wait(lambda: b'explicit-model' in bytes(a[2]), '--model was not reflected in native TUI')
        assert {s['session']['project'] for s in online()}=={'demo','child'}
        original = next(s['session']['native'] for s in online() if s['session']['project']=='demo')
        first = {s['session']['native'] for s in online()}
        send(a, 'Persist this session')
        wait(lambda: b'PROCESS-OK' in bytes(a[2]), 'mock model did not complete the initial task')
        def title_matches():
            try:
                with sqlite3.connect(root/'state/state.sqlite') as db:
                    return db.execute("SELECT t.title FROM session_titles t JOIN sessions s ON s.id=t.session WHERE s.native=?",[original]).fetchone()==('Process test',)
            except sqlite3.Error:
                return False
        wait(title_matches, 'automatic native title was not synchronized')

        send(a, '/new')
        wait(lambda: len(online()) == 2 and {s['session']['native'] for s in online()} != first, '/new did not deactivate old session')
        send(a, '/resume '+original)
        wait(lambda: any(s['session']['native']==original for s in online()), 'permission launch flags blocked /resume')
        # SIGKILL closes the lease; the terminal child must also disappear.
        stop(a, signal.SIGKILL)
        wait(lambda: len(online()) == 1, 'killed wrapper retained its session')
        stop(b)
        wait(lambda: not (root / 'run/control.sock').exists(), 'last wrapper left broker running')
        assert not list((root / 'run').glob('*/engine.sock'))
        for process in Path('/proc').iterdir():
            if process.name.isdigit():
                try:
                    assert str(root).encode() not in (process/'cmdline').read_bytes(), 'orphaned test process: '+process.name
                except OSError:
                    pass
        logs = (root / 'state/service.log').read_text()
        assert logs.count('uaoi service ready') == 1, logs
        assert 'Message too long' not in logs
        print('wrapper: parallel startup, native help, custom config, navigation, kill cleanup and idle shutdown passed')
    finally:
        for pid, terminal, _ in sessions:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.close(terminal)
        # Clean up even on assertion failure, without affecting other native processes.
        for process in Path('/proc').iterdir():
            if not process.name.isdigit():
                continue
            try:
                command=(process/'cmdline').read_bytes()
                if str(root).encode() in command and b'uaoi' in command:
                    os.kill(int(process.name),signal.SIGTERM)
            except (OSError,ProcessLookupError):
                pass
