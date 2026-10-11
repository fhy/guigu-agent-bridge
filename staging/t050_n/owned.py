"""Ordinary direct-child lifecycle; no historical process discovery."""
import ctypes
import os
import signal
import subprocess
import time


def subreaper():
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise RuntimeError("subreaper-unavailable")


class Owned:
    def __init__(self, argv, *, env=None, cwd=None, pipe=False, stderr_pipe=False):
        self.child = subprocess.Popen(argv, env=env, cwd=cwd,
                                      start_new_session=True,
                                      stdin=subprocess.PIPE if pipe else subprocess.DEVNULL,
                                      stdout=subprocess.PIPE if pipe else subprocess.DEVNULL,
                                      stderr=subprocess.PIPE if stderr_pipe else subprocess.DEVNULL)
        self.group = self.child.pid
        self.result = None

    def __enter__(self):
        return self

    def exited(self):
        # WNOWAIT preserves the leader until group termination, avoiding PGID reuse.
        return os.waitid(os.P_PID, self.child.pid,
                         os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None

    def stop(self, timeout=5):
        if self.result is not None:
            return self.result
        os.killpg(self.group, signal.SIGTERM)
        deadline = time.monotonic() + timeout
        while not self.exited() and time.monotonic() < deadline:
            time.sleep(0.05)
        # Terminate descendants too, before reaping the still-owned group leader.
        os.killpg(self.group, signal.SIGKILL)
        code = self.child.wait(timeout=3)
        reaped = 0
        deadline = time.monotonic() + 3
        while True:
            try:
                pid, _ = os.waitpid(-self.group, os.WNOHANG)
                if pid:
                    reaped += 1
                    continue
                if time.monotonic() >= deadline:
                    raise RuntimeError("owned-reap-timeout")
                time.sleep(0.02)
            except ChildProcessError:
                break
        self.result = {"exit": code, "descendants_reaped": reaped,
                       "cleanup": "owned-group-reaped"}
        if self.child.stdin:
            self.child.stdin.close()
        if self.child.stdout:
            self.child.stdout.close()
        if self.child.stderr:
            self.child.stderr.close()
        return self.result

    def __exit__(self, *_):
        self.stop()
