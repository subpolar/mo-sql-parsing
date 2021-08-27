# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import json

from mo_parsing.utils import listwrap

from mo_sql_parsing import parse


def normalize(expression):
    # ensure parameters are in a list
    if isinstance(expression, dict):
        return [
            {"op": op, "operands": [normalize(p) for p in listwrap(operands)]}
            for op, operands in expression.items()
        ][0]
    return expression


result = parse("select myFunction(a, b+c)")

print(json.dumps(normalize(result['select']['value'])))

print("done")
