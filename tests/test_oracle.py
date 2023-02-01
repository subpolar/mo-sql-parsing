# encoding: utf-8
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase

from mo_sql_parsing import parse


class TestOracle(TestCase):
    def test_issue_90_tablesample1(self):
        sql = "SELECT * FROM foo SAMPLE bernoulli (1) WHERE a < 42"
        result = parse(sql)
        expected = {
            "from": {
                "tablesample": {"method": "bernoulli", "percent": 1},
                "value": "foo",
            },
            "select": "*",
            "where": {"lt": ["a", 42]},
        }
        self.assertEqual(result, expected)

    def test_issue_90_tablesample2(self):
        sql = "SELECT * FROM foo SAMPLE(1) WHERE a < 42"
        result = parse(sql)
        expected = {
            "from": {"tablesample": {"percent": 1}, "value": "foo",},
            "select": "*",
            "where": {"lt": ["a", 42]},
        }
        self.assertEqual(result, expected)

    def test_issue_157_describe(self):
        sql = """describe into s.t@database for select * from temp"""
        result = parse(sql)
        expected = {"explain": {"from": "temp", "select": "*"}, "into": "s.t@database"}
        self.assertEqual(result, expected)
