# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import json
from threading import Lock

from mo_sql_parsing.sql_parser import SQLParser, scrub_literal, scrub
from mo_sql_parsing.utils import SQL_NULL

parseLocker = Lock()  # ENSURE ONLY ONE PARSING AT A TIME


def parse(sql, null=SQL_NULL):
    """
    :param sql: String of SQL
    :param null: What value to use as NULL (default is the null function `{"null":{}}`)
    :return: parse tree
    """
    with parseLocker:
        utils.null_locations = []
        sql = sql.rstrip().rstrip(";")
        parse_result = SQLParser.parseString(sql, parseAll=True)
        output = scrub(parse_result)
        if null is not SQL_NULL:
            for o, n in utils.null_locations:
                o[n] = null
        return output


def format(json, **kwargs):
    from mo_sql_parsing.formatting import Formatter

    return Formatter(**kwargs).format(json)


_ = json.dumps

__all__ = ["parse", "format"]
