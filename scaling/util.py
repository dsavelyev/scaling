import os
import threading
from enum import Enum
import re
import signal
import string

try:
    from msvcrt import kbhit, getch
except ImportError:
    import select
    import sys
    import termios
    unix = True
else:
    unix = False


class OrderedEnum(Enum):
    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


def param_to_string(param):
    if type(param) is int:
        return str(param)
    elif type(param) is float:
        return repr(param)
    elif type(param) is str:
        return param
    else:
        raise TypeError('unsupported type {0} for parameter'.format(
            type(param).__name__))


def to_bytes(s, encoding='utf-8'):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return s.encode(encoding)
    else:
        raise TypeError


class RepeatingTimerThread:
    def __init__(self, interval, cb, daemon=True):
        self.evt = threading.Event()
        self.thrd = threading.Thread(target=self._thread_body, daemon=daemon)
        self.interval = interval
        self.cb = cb

        self.thrd.start()

    def _thread_body(self):
        while not self.evt.is_set():
            self.cb()
            self.evt.wait(self.interval)

    def stop(self):
        self.evt.set()
        self.thrd.join()


class _stdin_noecho_unix:
    def __enter__(self):
        if os.isatty(0):
            self._old_params = termios.tcgetattr(0)
            new_params = self._old_params[:]
            new_params[3] &= ~(termios.ICANON | termios.ECHO)
            new_params[6][termios.VMIN] = 0
            new_params[6][termios.VTIME] = 0

            termios.tcsetattr(0, termios.TCSADRAIN, new_params)

    def __exit__(self, *args):
        if os.isatty(0):
            termios.tcsetattr(0, termios.TCSADRAIN, self._old_params)


if unix:
    stdin_noecho = _stdin_noecho_unix

    def poll_keyboard():
        ready, _, _ = select.select([0], [], [], 0)
        if 0 in ready:
            return sys.stdin.buffer.read(1)
        else:
            return None
else:
    class stdin_noecho:
        def __enter__(self):
            pass

        def __exit__(self, *args):
            pass

    def poll_keyboard():
        if kbhit():
            ch = getch()
            if ch in (b'\000', b'\xe0'):
                getch()
                return None
            return ch
        else:
            return None


class handle_sigint:
    def __init__(self, handler):
        self._handler = handler

    def __enter__(self):
        self._old_handler = signal.signal(signal.SIGINT, self._handler)

    def __exit__(self, *args):
        signal.signal(signal.SIGINT, self._old_handler)


def call_later(timeout, cancel_evt, func, *args, **kwargs):
    cancel_evt.wait(timeout=timeout)
    if cancel_evt.is_set():
        return
    return func(*args, **kwargs)
