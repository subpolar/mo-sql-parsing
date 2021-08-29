# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_future import sort_using_key, first
from mo_parsing import listwrap

MAX = 10000

_operators = [
    "select",
    "trim",
]

op_order = {op: i for i, op in enumerate(_operators)}


def normalize(expr):
    if isinstance(expr, dict):
        items = list(expr.items())
        if len(items) == 0:
            return {}

        best_op = first(sort_using_key(((op_order.get(op, MAX), op) for op, _ in items), lambda p: p[0]))
        args = [normalize(e) for e in listwrap(expr[best_op])]
        kwargs = {name: normalize(e) for name, e in items if name != best_op}
        return {
            "op": best_op,
            "args": args,
            "kwargs": kwargs
        }
    elif isinstance(expr, list):
        return [normalize(e) for e in expr]
    return expr




