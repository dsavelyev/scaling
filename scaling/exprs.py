import operator
import re
from collections import namedtuple
from functools import partial

import ply.lex as lex
# from ply.lex import TOKEN
import ply.yacc as yacc
from toposort import toposort_flatten

from .functions import builtin_funcs, FunctionError

# AST definitions
BinOp = namedtuple('BinOp', ['lhs', 'op', 'rhs'])
UnOp = namedtuple('UnOp', ['op', 'inner'])
FuncCall = namedtuple('FuncCall', ['funcname', 'args'])
IdentRef = namedtuple('IdentRef', ['ident'])

Expression = namedtuple('Expression', ['expr', 'idents'])


class ParamSpecError(Exception):
    pass


class ParamSpecParser:
    # --- Lexer ---
    tokens = [
        'IDENT', 'PLUS', 'MINUS', 'TIMES', 'DIVIDE', 'POW', 'MOD', 'NUMBER',
        'LPAREN', 'RPAREN', 'COMMA', 'FUNCNAME', 'LBRACKET', 'RBRACKET',
        'COLON', 'EXPRSEP', 'STRING'
    ]

    t_PLUS = r'\+'
    t_MINUS = r'-'
    t_TIMES = r'\*'
    t_DIVIDE = r'/'
    t_MOD = r'%'
    t_LPAREN = r'\('
    t_RPAREN = r'\)'
    t_POW = r'\^'
    t_COMMA = r','
    t_LBRACKET = r'\['
    t_RBRACKET = r'\]'
    t_COLON = r':'
    t_EXPRSEP = r';'

    def __init__(self, single=False, debug=False):
        self.lexer = lex.lex(module=self, debug=debug)
        self.parser = yacc.yacc(
            module=self,
            debug=debug,
            write_tables=False,
            start=self.start if not single else 'expression')

    def parse(self, s):
        return self.parser.parse(s, lexer=self.lexer)

    def t_NUMBER(self, t):
        r'\d+(?P<fracpart>\.\d+)?(?P<exponent>[eE][+-]?\d+)?'
        if t.lexer.lexmatch.group('fracpart') is None \
           and t.lexer.lexmatch.group('exponent') is None:
            t.value = int(t.value)
        else:
            t.value = float(t.value)
        return t

    def t_STRING(self, t):
        r'"(?P<contents>(?:\\"|\\\\|[^\\"])*)"'
        contents = t.lexer.lexmatch.group('contents')
        contents = re.sub(r'\\(\\|")', lambda match: match.group(1), contents)
        t.value = contents
        return t

    ident_regex = r'[A-Za-z_][A-Za-z0-9_]*'

    @lex.TOKEN(ident_regex)
    def t_IDENT(self, t):
        if t.value not in builtin_funcs.keys():
            t.type = 'IDENT'
        else:
            t.type = 'FUNCNAME'
        return t

    def t_error(self, t):
        raise ParamSpecError('Lexer error on %s' % t)

    t_ignore = ' \t\n'

    # --- Parser ---

    precedence = (('left', 'PLUS', 'MINUS'),
                  ('left', 'TIMES', 'DIVIDE', 'MOD'),
                  ('right', 'POW'),
                  ('right', 'UMINUS'))

    start = 'expressionlist'

    def p_expressionlist_1(self, p):
        '''expressionlist : exprlistelem'''
        p[0] = p[1]

    def p_expressionlist_last(self, p):
        '''expressionlist : expressionlist EXPRSEP exprlistelem'''
        for key in p[3]:
            if key in p[1]:
                raise ParamSpecError(f'redefinition of {key}')
            p[1][key] = p[3][key]
        p[0] = p[1]

    def p_exprlistelem(self, p):
        '''exprlistelem : identlist COLON expression'''
        p[0] = {tuple(p[1]): p[3]}

    def p_exprlistelem_empty(self, p):
        '''exprlistelem :'''
        p[0] = {}

    def p_expression_binop(self, p):
        '''expression : expression PLUS expression
                      | expression MINUS expression
                      | expression TIMES expression
                      | expression DIVIDE expression
                      | expression POW expression
                      | expression MOD expression'''
        p[0] = Expression(
            BinOp(p[1].expr, p[2], p[3].expr), p[1].idents + p[3].idents)

    def p_expression_uminus(self, p):
        '''expression : MINUS expression %prec UMINUS'''
        p[0] = Expression(UnOp('-', p[2].expr), p[2].idents)

    def p_expression_parens(self, p):
        '''expression : LPAREN expression RPAREN'''
        p[0] = p[2].expr

    def p_expression_literal(self, p):
        '''expression : NUMBER
                      | STRING'''
        p[0] = Expression(p[1], [])

    def p_expression_ident(self, p):
        '''expression : IDENT'''
        p[0] = Expression(IdentRef(p[1]), [p[1]])

    def p_expression_funccall(self, p):
        '''expression : FUNCNAME LPAREN argv RPAREN'''
        p[0] = Expression(FuncCall(p[1], p[3].expr), p[3].idents)

    def p_expression_list(self, p):
        '''expression : LBRACKET argv RBRACKET'''
        p[0] = p[2]

    def p_argv_empty(self, p):
        '''argv : '''
        p[0] = Expression([], [])

    def p_argv_single(self, p):
        '''argv : argv1 expression'''
        p[0] = Expression(p[1].expr + [p[2].expr], p[1].idents + p[2].idents)

    def p_argv1_empty(self, p):
        '''argv1 : '''
        p[0] = Expression([], [])

    def p_argv1_comma(self, p):
        '''argv1 : argv1 expression COMMA'''
        p[0] = Expression(p[1].expr + [p[2].expr], p[1].idents + p[2].idents)

    def p_identlist_single(self, p):
        '''identlist : IDENT'''
        p[0] = [p[1]]

    def p_identlist_comma(self, p):
        '''identlist : identlist COMMA IDENT'''
        p[0] = p[1] + [p[3]]

    def p_error(self, t):
        raise ParamSpecError('Syntax error near token {0}'.format(t))


_binopfuncs = {
    '+': operator.add,
    '-': operator.sub,
    '*': operator.mul,
    '/': lambda x, y: x // y if isinstance(x, int) and isinstance(y, int)\
                      else x / y,
    '^': operator.pow,
    '%': operator.mod
}

_unopfuncs = {'-': operator.neg}


def _real_eval_expr(expr, expr_vars):
    if isinstance(expr, BinOp):
        return _binopfuncs[expr.op](_real_eval_expr(expr.lhs, expr_vars),
                                    _real_eval_expr(expr.rhs, expr_vars))
    elif isinstance(expr, UnOp):
        return _unopfuncs[expr.op](_real_eval_expr(expr.inner, expr_vars))
    elif isinstance(expr, FuncCall):
        return builtin_funcs[expr.funcname](
            *map(lambda e: _real_eval_expr(e, expr_vars), expr.args))
    elif isinstance(expr, IdentRef):
        return expr_vars[expr.ident]
    elif isinstance(expr, int) or isinstance(expr, float) or isinstance(expr, str):
        return expr
    elif isinstance(expr, list):
        return [_real_eval_expr(x, expr_vars) for x in expr]
    else:
        assert False, f'unknown expression type: {type(expr)}'


def _is_iter(x):
    if isinstance(x, str):
        return False
    try:
        iter(x)
    except TypeError:
        return False
    else:
        return True


def eval_expr(names, expr, expr_vars):
    '''
    Evaluates an expression in the form of an AST.
    '''
    ret = _real_eval_expr(expr, expr_vars)

    # lift single values into iterables
    if not _is_iter(ret):
        ret = (ret, )

    def destructure(el):
        if not _is_iter(el):
            el = (el, )
        if len(names) != len(el):
            raise ParamSpecError('tuple {2}: expected {0} values, got {1}'.format(
                len(names), len(el), names))
        return {name: value for name, value in zip(names, el)}

    return map(destructure, ret)


def _iterfunc_product(funcs):
    '''
    Given a topologically-sorted iterable of functions, each of which takes a
    dict of the currently bound variables of a point in the parameter space and
    returns an iterable of dicts with new variable bindings, iterates over the entire
    parameter space.
    '''

    numfuncs = len(funcs)
    if numfuncs == 0:
        return

    values = {}
    stk = [funcs[0](values)]

    while len(stk) != 0:
        try:
            nextvalues = next(stk[-1])
        except StopIteration:
            stk.pop()
        else:
            values.update(nextvalues)
            index = len(stk)
            if index >= numfuncs:
                yield values.copy()
            else:
                stk.append(funcs[index](values))


def gen_params(param_spec):
    '''
    Iterates over all the combinations of parameters specified by param_spec.
    '''

    depends = {}  # dependencies between *tuples* of names
    tuplemap = {}  # name -> tuple in which it appeared

    orig_keys = set()
    expr_keys = set()

    psp = ParamSpecParser()

    try:
        asts = psp.parse(param_spec)
    except FunctionError as e:
        raise ParamSpecError(str(e))

    for identlist, expr in asts.items():
        for ident in identlist:
            # map idents to the tuples that contain them
            tuplemap[ident] = identlist
            orig_keys.add(ident)

        depends[identlist] = expr.idents
        expr_keys.update(expr.idents)

    if not expr_keys <= orig_keys:
        raise ParamSpecError(f'unknown vars: {expr_keys - orig_keys}')

    for k, v in depends.items():
        depends[k] = set(map(tuplemap.__getitem__, v))

    keys = toposort_flatten(depends, sort=False)

    return _iterfunc_product(
        [partial(eval_expr, k, asts[k].expr) for k in keys])
