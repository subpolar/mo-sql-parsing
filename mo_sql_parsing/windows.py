# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from mo_parsing.helpers import delimited_list

from mo_sql_parsing.keywords import *
from mo_sql_parsing.utils import *


# https://docs.microsoft.com/en-us/sql/t-sql/queries/select-over-clause-transact-sql?view=sql-server-ver15


def _to_bound_call(tokens):
    zero = tokens["zero"]
    if zero:
        return {"min": 0, "max": 0}

    direction = scrub(tokens["direction"])
    limit = scrub(tokens["limit"])
    if direction == "preceding":
        if limit == "unbounded":
            return {"max": 0}
        else:
            return {"min": -limit, "max": 0}
    else:  # following
        if limit == "unbounded":
            return {"min": 0}
        else:
            return {"min": 0, "max": limit}


def _to_between_call(tokens):
    minn = scrub(tokens["min"])
    maxx = scrub(tokens["max"])

    if maxx.get("max") == 0:
        # following
        return {
            "min": minn.get("min"),
            "max": maxx.get("min"),
        }
    elif minn.get("min") == 0:
        # preceding
        return {"min": minn.get("max"), "max": maxx.get("max")}
    else:
        return {
            "min": minn.get("min"),
            "max": maxx.get("max"),
        }


UNBOUNDED = keyword("unbounded")
PRECEDING = keyword("preceding")
FOLLOWING = keyword("following")
CURRENT_ROW = keyword("current row")
ROWS = keyword("rows")
RANGE = keyword("range")

bound = (
    CURRENT_ROW("zero")
    | (UNBOUNDED | int_num)("limit") + (PRECEDING | FOLLOWING)("direction")
) / _to_bound_call
between = (BETWEEN + bound("min") + AND + bound("max")) / _to_between_call

row_clause = (ROWS | RANGE).suppress() + (between | bound)


def window(expr, sort_column):
    return (
        # Optional((keyword("ignore nulls"))("ignore_nulls") / (lambda: True))
        Optional(
            WITHIN_GROUP
            + LB
            + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby"))
            + RB
        )("within")
        + Optional(
            OVER
            + LB
            + Optional(PARTITION_BY + delimited_list(Group(expr))("partitionby"))
            + Optional(
                ORDER_BY
                + delimited_list(Group(sort_column))("orderby")
                + Optional(row_clause)("range")
            )
            + RB
        )("over")
    )
