import threading
from enum import Enum
import re
import string


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
