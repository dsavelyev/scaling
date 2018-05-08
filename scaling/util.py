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


def poll_keyboard_windows():
    if kbhit():
        return getch()
    else:
        return None


def poll_keyboard_unix():
    if os.isatty(0):
        old_params = termios.tcgetattr(0)
        new_params = old_params[:]
        new_params[3] &= ~(termios.ICANON | termios.ECHO)
        new_params[6][termios.VMIN] = 0
        new_params[6][termios.VTIME] = 0

    try:
        if os.isatty(0):
            termios.tcsetattr(0, termios.TCSADRAIN, new_params)

        ready, _, _ = select.select([0], [], [], 0)
        if 0 in ready:
            return sys.stdin.read(1)
        else:
            return None
    finally:
        if os.isatty(0):
            termios.tcsetattr(0, termios.TCSADRAIN, old_params)


def poll_keyboard():
    if unix:
        return poll_keyboard_unix()
    else:
        return poll_keyboard_windows()


class handle_sigint:
    def __init__(self, handler):
        self._handler = handler

    def __enter__(self):
        self._old_handler = signal.signal(signal.SIGINT, self._handler)

    def __exit__(self, *args):
        signal.signal(signal.SIGINT, self._old_handler)
