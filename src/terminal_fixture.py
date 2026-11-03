"""PTY driver for the opt-in native terminal integration test."""
import json
import fcntl
import os
import pty
import select
import signal
import sys
import struct
import termios

command = json.loads(os.environ["UAOI_TERMINAL_COMMAND"])
pid, terminal = pty.fork()
if pid == 0:
    os.execvpe(command[0], command, os.environ)
fcntl.ioctl(terminal, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
try:
    while True:
        ready, _, _ = select.select([terminal, sys.stdin.fileno()], [], [], 1)
        if terminal in ready:
            try:
                data = os.read(terminal, 65536)
            except OSError:
                break
            if not data:
                break
            os.write(sys.stdout.fileno(), data)
            with open(os.environ["UAOI_TERMINAL_LOG"], "ab") as log:
                log.write(data)
            for query, answer in [
                (b"\x1b[6n", b"\x1b[1;1R"),
                (b"\x1b[c", b"\x1b[?1;2c"),
                (b"\x1b]10;?", b"\x1b]10;rgb:ffff/ffff/ffff\x1b\\"),
                (b"\x1b]11;?", b"\x1b]11;rgb:0000/0000/0000\x1b\\"),
            ]:
                if query in data:
                    os.write(terminal, answer)
        if sys.stdin.fileno() in ready:
            data = os.read(sys.stdin.fileno(), 65536)
            if not data:
                break
            os.write(terminal, data)
finally:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    os.close(terminal)
    os.waitpid(pid, 0)
