# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_dots import from_data, to_data, dict_to_data, Data
from mo_future import sort_using_key, first, is_text
from mo_logs import Log
from mo_parsing import listwrap

MAX = 10000

_operators = [
    "with",
    "from",
    "select",
    "left join",
    "when",
    "trim",
]


op_order = {op: i for i, op in enumerate(_operators)}


def normalize(expr):
    """
    RETURN {"op": op, "args": args, "kwargs": kwargs} FORMAT
    :param expr: JSON expression
    """
    if expr == None:
        return None

    expr = from_data(expr)
    if isinstance(expr, dict):
        items = list(expr.items())
        if len(items) == 0:
            return {}

        lookup = [(op_order.get(op, MAX), op) for op, _ in items]
        if len(lookup) > 1 and all(l[0] is MAX for l in lookup):
            Log.error("do not know how to handle {{keys}}", keys=expr.keys())

        _, best_op = first(sort_using_key(lookup, lambda p: p[0]))
        return _special.get(best_op, normalize_expression)(best_op, expr)
    elif isinstance(expr, list):
        return [normalize(e) for e in expr]
    return expr


def normalize_expression(op, expr):
    args = [normalize(e) for e in listwrap(expr[op])]
    kwargs = {
        sub_op: normalize({sub_op: e})["args"]
        for sub_op, e in expr.items()
        if sub_op != op
    }
    if kwargs:
        return {"op": op, "args": args, "kwargs": kwargs}
    else:
        return {"op": op, "args": args}


def normalize_with(op, expr):
    args = expr.copy()
    del args[op]

    return {
        "op": op,
        "args": [normalize(args)],
        "kwargs": {"assign": [
            {"name": p["name"], "value": normalize(p["value"])}
            for p in listwrap(expr[op])
        ]},
    }


def normalize_from(op, expr):
    def norm_source(d):
        if is_text(d):
            return d
        name = d.get("name")
        if name != None:
            return {"name": name, "value": normalize(d["value"])}
        value = d.get("value")
        if value != None:
            return normalize(d["value"])
        return normalize(d)

    def norm_join(lhs, op, expr):
        expr = to_data(expr)
        output = dict_to_data({"op": op, "args": [lhs, norm_source(expr[op])]})

        output.kwargs.on = normalize(expr.on)
        output.kwargs.using = normalize(expr.using)
        return from_data(output)

    sources = listwrap(expr[op])
    lhs = norm_source(sources[0])
    for s in sources[1:]:
        if is_text(s):
            lhs = {"op": "outer join", "args": [lhs, s]}
        else:
            join_op = list(s.keys() - {"on", "using"})[0]
            lhs = norm_join(lhs, join_op, s)

    kwargs = {
        sub_op: normalize({sub_op: e})["args"]
        for sub_op, e in expr.items()
        if sub_op != op
    }

    if kwargs:
        return {"op": "from", "args": [lhs], "kwargs": kwargs}
    else:
        return {"op": "from", "args": [lhs], }


def normalize_orderby(op, expr):
    def norm(d):
        if is_text(d):
            return d
        output = {}
        name = d.get("sort")
        if name != None:
            output["sort"] = name
        value = d.get("value")
        if value != None:
            return normalize(d["value"])
        return normalize(d)

    args = [norm(p) for p in listwrap(expr[op])]

    return {"op": op, "args": args}


def normalize_select(op, expr):
    kwargs = {
        sub_op: normalize({sub_op: e})["args"]
        for sub_op, e in expr.items()
        if sub_op not in [op, "top"]
    }
    to_data(kwargs).top = to_data(expr).top

    def norm(d):
        if is_text(d):
            return d
        d = to_data(d)
        output = Data()

        output.name = d.name
        output.value = normalize(from_data(d.value))
        output.over = d.over
        return from_data(output)

    args = [norm(p) for p in listwrap(expr[op])]

    if kwargs:
        return {"op": op, "args": args, "kwargs": kwargs}
    else:
        return {"op": op, "args": args}


def normalize_create_table(op, expr):
    kwargs = expr.copy()
    del kwargs[op]
    return {"op": op, "kwargs": kwargs}


_special = {
    "with": normalize_with,
    "select": normalize_select,
    "distinct": normalize_select,
    "from": normalize_from,
    "orderby": normalize_orderby,
    "having": normalize_orderby,
    "create table": normalize_create_table,
}
