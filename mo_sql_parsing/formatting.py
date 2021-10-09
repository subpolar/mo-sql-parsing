# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Beto Dealmeida (beto@dealmeida.net)
#

from __future__ import absolute_import, division, unicode_literals

import re

from mo_dots import split_field
from mo_future import first, is_text, long, string_types, text
from mo_parsing import listwrap

from mo_sql_parsing.keywords import RESERVED, join_keywords, precedence
from mo_sql_parsing.utils import binary_ops

MAX_PRECEDENCE = 100
VALID = re.compile(r"^[a-zA-Z_]\w*$")


def is_keyword(identifier):
    try:
        RESERVED.parseString(identifier)
        return True
    except Exception:
        return False


def should_quote(identifier):
    """
    Return true if a given identifier should be quoted.

    This is usually true when the identifier:

      - is a reserved word
      - contain spaces
      - does not match the regex `[a-zA-Z_]\\w*`

    """
    return identifier != "*" and (not VALID.match(identifier) or is_keyword(identifier))


def escape(ident, ansi_quotes, should_quote):
    """
    Escape identifiers.

    ANSI uses double quotes, but many databases use back quotes.

    """

    def esc(identifier):
        if not should_quote(identifier):
            return identifier

        quote = '"' if ansi_quotes else "`"
        identifier = identifier.replace(quote, 2 * quote)
        return "{0}{1}{2}".format(quote, identifier, quote)

    return ".".join(esc(f) for f in split_field(ident))


def Operator(_op):
    prec = precedence[binary_ops[_op]]
    op = " {0} ".format(_op).upper()

    def func(self, json):
        acc = []

        if isinstance(json, dict):
            # {VARIABLE: VALUE} FORM
            k, v = first(json.items())
            json = [k, {"literal": v}]

        for v in json if isinstance(json, list) else [json]:
            sql = self.format(v)
            if isinstance(v, (text, int, float, long)):
                acc.append(sql)
                continue
            if v is None:
                acc.append("NULL")
                continue
            if isinstance(v, list):
                acc.append(sql)
                continue
            acc.append(isolate(v, sql, prec))
        return op.join(acc)

    return func


def isolate(expr, sql, prec):
    """
    RETURN sql IN PARENTHESIS IF PREEDENCE > prec
    :param expr: expression to isolate
    :param sql: sql to return
    :param prec: current precedence
    """
    if is_text(expr):
        return sql
    ps = [p for k in expr.keys() for p in [precedence.get(k)] if p is not None]
    if not ps:
        return sql
    elif min(ps) >= prec:
        return f"({sql})"
    else:
        return sql


class Formatter:

    clauses = [
        "with_",
        "select",
        "select_distinct",
        "from_",
        "where",
        "groupby",
        "having",
        "orderby",
        "limit",
        "offset",
    ]

    # infix operators
    _concat = Operator("||")
    _mul = Operator("*")
    _div = Operator("/")
    _mod = Operator("%")
    _add = Operator("+")
    _sub = Operator("-")
    _neq = Operator("<>")
    _gt = Operator(">")
    _lt = Operator("<")
    _gte = Operator(">=")
    _lte = Operator("<=")
    _eq = Operator("=")
    _in = Operator("in")
    _nin = Operator("not in")
    _or = Operator("or")
    _and = Operator("and")
    _binary_and = Operator("&")
    _binary_or = Operator("|")
    _union = Operator("union")
    _union_all = Operator("union all")
    _intersect = Operator("intersect")
    _minus = Operator("minus")
    _except = Operator("except")

    def __init__(self, ansi_quotes=True, should_quote=should_quote):
        self.ansi_quotes = ansi_quotes
        self.should_quote = should_quote

    def format(self, json):
        if isinstance(json, list):
            return self.sql_list(json)
        if isinstance(json, dict):
            if len(json) == 0:
                return ""
            elif "value" in json:
                return self.value(json)
            elif any(clause in json for clause in ("select", "select_distinct", "from")):
                return self.query(json)
            elif "null" in json:
                return "NULL"
            else:
                return self.op(json)
        if isinstance(json, string_types):
            return escape(json, self.ansi_quotes, self.should_quote)
        if json == None:
            return "NULL"

        return text(json)

    def sql_list(self, json):
        return "(" + ", ".join(self.format(element) for element in json) + ")"

    def value(self, json):
        parts = [self.format(json["value"])]
        if "name" in json:
            parts.extend(["AS", self.format(json["name"])])
        return " ".join(parts)

    def op(self, json):

        if len(json) > 1:
            raise Exception("Operators should have only one key!")
        key, value = list(json.items())[0]

        # check if the attribute exists, and call the corresponding method;
        # note that we disallow keys that start with `_` to avoid giving access
        # to magic methods
        attr = f"_{key}"
        if hasattr(self, attr) and not key.startswith("_"):
            method = getattr(self, attr)
            return method(value)

        # treat as regular function call
        if isinstance(value, dict) and len(value) == 0:
            return (
                key.upper() + "()"
            )  # NOT SURE IF AN EMPTY dict SHOULD BE DELT WITH HERE, OR IN self.format()
        else:
            params = ", ".join(self.format(p) for p in listwrap(value))
            return f"{key.upper()}({params})"

    def _binary_not(self, value):
        return "~{0}".format(self.format(value))

    def _exists(self, value):
        return "{0} IS NOT NULL".format(self.format(value))

    def _missing(self, value):
        return "{0} IS NULL".format(self.format(value))

    def _like(self, pair):
        return "{0} LIKE {1}".format(self.format(pair[0]), self.format(pair[1]))

    def _not_like(self, pair):
        return "{0} NOT LIKE {1}".format(self.format(pair[0]), self.format(pair[1]))

    def _rlike(self, pair):
        return "{0} RLIKE {1}".format(self.format(pair[0]), self.format(pair[1]))

    def _not_rlike(self, pair):
        return "{0} NOT RLIKE {1}".format(
            self.format(pair[0]), self.format(pair[1])
        )

    def _is(self, pair):
        return "{0} IS {1}".format(self.format(pair[0]), self.format(pair[1]))

    def _collate(self, pair):
        return "{0} COLLATE {1}".format(self.format(pair[0]), pair[1])

    def _case(self, checks):
        parts = ["CASE"]
        for check in checks if isinstance(checks, list) else [checks]:
            if isinstance(check, dict):
                if "when" in check and "then" in check:
                    parts.extend(["WHEN", self.format(check["when"])])
                    parts.extend(["THEN", self.format(check["then"])])
                else:
                    parts.extend(["ELSE", self.format(check)])
            else:
                parts.extend(["ELSE", self.format(check)])
        parts.append("END")
        return " ".join(parts)

    def _literal(self, json):
        if isinstance(json, list):
            return "({0})".format(", ".join(self._literal(v) for v in json))
        elif isinstance(json, string_types):
            return "'{0}'".format(json.replace("'", "''"))
        else:
            return str(json)

    def _between(self, json):
        return "{0} BETWEEN {1} AND {2}".format(
            self.format(json[0]), self.format(json[1]), self.format(json[2])
        )

    def _not_between(self, json):
        return "{0} NOT BETWEEN {1} AND {2}".format(
            self.format(json[0]), self.format(json[1]), self.format(json[2])
        )

    def _join_on(self, json):
        detected_join = join_keywords & set(json.keys())
        if len(detected_join) == 0:
            raise Exception(
                'Fail to detect join type! Detected: "{}" Except one of: "{}"'.format(
                    [on_keyword for on_keyword in json if on_keyword != "on"][0],
                    '", "'.join(join_keywords),
                )
            )

        join_keyword = detected_join.pop()

        acc = []
        acc.append(join_keyword.upper())
        acc.append(self.format(json[join_keyword]))

        if json.get("on"):
            acc.append("ON")
            acc.append(self.format(json["on"]))
        if json.get("using"):
            acc.append("USING")
            acc.append(self.format(json["using"]))
        return " ".join(acc)

    def query(self, json):
        return " ".join(
            part
            for clause in self.clauses
            for part in [getattr(self, clause)(json)]
            if part
        )

    def with_(self, json):
        if "with" in json:
            with_ = json["with"]
            if not isinstance(with_, list):
                with_ = [with_]
            parts = ", ".join(
                "{0} AS ({1})".format(part["name"], self.format(part["value"]))
                for part in with_
            )
            return "WITH {0}".format(parts)

    def select(self, json):
        if "select" in json:
            param = ", ".join(self.format(s) for s in listwrap(json["select"]))
            if "top" in json:
                top = self.format(json["top"])
                return f"SELECT TOP ({top}) {param}"
            else:
                return f"SELECT {param}"

    def select_distinct(self, json):
        if "select_distinct" in json:
            return "SELECT DISTINCT {0}".format(self.format(json["select_distinct"]))

    def from_(self, json):
        if "from" in json:
            from_ = json["from"]
            sql = self.format(from_)

            return f"FROM {isolate(from_, sql, 30)}"

    def where(self, json):
        if "where" in json:
            return "WHERE {0}".format(self.format(json["where"]))

    def groupby(self, json):
        if "groupby" in json:
            param = ", ".join(self.format(s) for s in listwrap(json["groupby"]))
            return f"GROUP BY {param}"

    def having(self, json):
        if "having" in json:
            return "HAVING {0}".format(self.format(json["having"]))

    def orderby(self, json):
        if "orderby" in json:
            param = ", ".join(
                (self.format(s["value"]) + " " + s.get("sort", "").upper()).strip()
                for s in listwrap(json["orderby"])
            )
            return f"ORDER BY {param}"

    def limit(self, json):
        if "limit" in json:
            if json["limit"]:
                return "LIMIT {0}".format(self.format(json["limit"]))

    def offset(self, json):
        if "offset" in json:
            return "OFFSET {0}".format(self.format(json["offset"]))
