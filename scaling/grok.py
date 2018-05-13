import io
import logging

import regex


_logger = logging.getLogger(__name__)


_patterns = r'''
INT [+-]?[0-9]+
FLOAT (?<![0-9.+-])(?>[+-]?(?:[0-9]+(?:\.[0-9]*)?|(?:\.[0-9]+))(?:[eE][+-]?[0-9]+)?)
QUOTEDSTRING (?:(?<!\\)(?:"(?:\\.|[^\\"])*"|(?:'(?:\\.|[^\\'])*')|(?:`(?:\\.|[^\\`])*`)))
'''

_pattern_dict = {}


def _init_pattern_dict():
    pattern_regex = regex.compile(r'(\w+)\s+(.*)')

    for line in _patterns.splitlines():
        if line:
            sre_match = regex.match(pattern_regex, line)
            _pattern_dict[sre_match.group(1)] = sre_match.group(2)


_init_pattern_dict()


class GrokError(Exception):
    pass


class Grok:
    '''
    Implements regular expressions extended with a syntax inspired by
    Logstash's Grok plugin: %{PATTERN:name} becomes (?P<name>pattern). A
    literal percent character needs to be escaped like so: %%. See
    ``_patterns`` for the supported patterns.
    '''

    @staticmethod
    def parse_grok_pat(p, pat_dict, re_escape=False):
        pat_ref = io.StringIO()

        nameset = set()

        def add_to_pat_ref(c):
            nonlocal pat_ref
            pat_ref.write(c)
            return ('', PAT_REF)

        def parse_pat_ref(_):
            nonlocal pat_ref
            pat_ref_value = pat_ref.getvalue()
            spl = pat_ref_value.split(':')
            if not 1 <= len(spl) <= 2:
                raise GrokError(
                    f'invalid pattern reference: "{pat_ref_value}"')
            patname = spl[0]
            name = spl[1] if len(spl) == 2 else ''
            try:
                pattern = pat_dict[patname]
            except KeyError:
                raise GrokError(f'unknown pattern name "{patname}"')
            if name:
                nameset.add(name)
            pat_ref = io.StringIO()
            return (f'(?P<{name}>{pattern})' if name else pattern, INIT)

        maybe_escape = regex.escape if re_escape else lambda x: x

        # 'read_char': ('written_char', NEW_STATE)
        INIT, PERCENT, PAT_REF = range(3)
        transducer = (
            # INIT:
            {
                '%': ('', PERCENT),
                None: lambda c: (maybe_escape(c), INIT)
            },
            # PERCENT:
            {
                '%': (maybe_escape('%'), INIT),
                '{': ('', PAT_REF),
            },
            # PAT_REF:
            {
                '}': parse_pat_ref,
                None: add_to_pat_ref
            },
        )

        state = INIT
        out = io.StringIO()

        for i, c in enumerate(p):
            statedict = transducer[state]
            transition = statedict.get(c, statedict.get(None))
            if transition is None:
                raise GrokError(f'invalid char "{c}" at position {i}')
            if callable(transition):
                transition = transition(c)
            newout, state = transition
            out.write(newout)

        if state != INIT:
            raise GrokError('unterminated percent sequence')

        return out.getvalue(), nameset

    def __init__(self, pattern, re_escape=False):
        re_pattern, self._nameset = Grok.parse_grok_pat(
            pattern, _pattern_dict, re_escape)
        _logger.debug(re_pattern)
        self._compiled_regex = regex.compile(re_pattern)

    def search(self, string):
        return regex.search(self._compiled_regex, string)

    def names():
        return self._nameset
