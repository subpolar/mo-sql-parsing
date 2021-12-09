# encoding: utf-8
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase

from mo_parsing.debug import Debugger

from mo_sql_parsing import parse


class TestAthena(TestCase):
    def test_issue_55_unnest(self):
        # UNNEST without AS is not understood.
        sql = """SELECT * FROM UNNEST(ARRAY[foo,bar]) table_name(column_name)"""
        result = parse(sql)
        expected = {
            "from": {
                "name": {"table_name": "column_name"},
                "value": {"unnest": {"create_array": ["foo", "bar"]}},
            },
            "select": "*",
        }
        self.assertEqual(result, expected)

    def test_issue_56_nulls_first(self):
        sql = """SELECT X() OVER (ORDER BY foo DESC NULLS FIRST)"""
        result = parse(sql)
        expected = {"select": {
            "over": {"orderby": {"nulls": "first", "sort": "desc", "value": "foo"}},
            "value": {"x": {}},
        }}
        self.assertEqual(result, expected)

    def test_issue_57_window_in_expression(self):
        sql = """SELECT X() OVER () = 1 AS is_true"""
        result = parse(sql)
        expected = {"select": {
            "name": "is_true",
            "value": {"eq": [{"over": {}, "value": {"x": {}}}, 1]},
        }}
        self.assertEqual(result, expected)

    def test_issue_58_filter_on_aggregate(self):
        sql = """SELECT MAX(1) FILTER (WHERE 1=1) AS foo"""
        result = parse(sql)
        expected = {"select": {
            "filter": {"eq": [1, 1]},
            "name": "foo",
            "value": {"max": 1},
        }}
        self.assertEqual(result, expected)

    def test_issue_60_row_type(self):
        # eg CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))
        sql = """SELECT CAST(x AS ROW(y VARCHAR))"""
        result = parse(sql)
        expected = {"select": {"value": {"cast": [
            "x",
            {"row": {"name": "y", "type": {"varchar": {}}}},
        ]}}}
        self.assertEqual(result, expected)

    def test_issue_61_dot(self):
        sql = """SELECT b['c'].d"""
        result = parse(sql)
        expected = {"select": {"value": {"get": [
            {"get": ["b", {"literal": "c"}]},
            "d",
        ]}}}
        self.assertEqual(result, expected)

    def test_issue_59_is_distinct_from(self):
        # https://prestodb.io/docs/current/functions/comparison.html#is-distinct-from-and-is-not-distinct-from
        sql = """SELECT 1 IS DISTINCT FROM 2"""
        result = parse(sql)
        expected = {"select": {"value": {"eq!": [1, 2]}}}
        self.assertEqual(result, expected)

    def test_issue_59_is_not_distinct_from(self):
        sql = """SELECT 1 IS NOT DISTINCT FROM 2"""
        result = parse(sql)
        expected = {"select": {"value": {"ne!": [1, 2]}}}
        self.assertEqual(result, expected)

    def test_issue_62_structuralA(self):
        sql = """SELECT CAST(x AS ARRAY(ROW(y VARCHAR)))"""
        result = parse(sql)
        expected = {"select": {"value": {"cast": [
            "x",
            {"array": {"row": {"name": "y", "type": {"varchar": {}}}}},
        ]}}}
        self.assertEqual(result, expected)

    def test_issue_62_structuralB(self):
        sql = """SELECT CAST(x AS ROW(y ARRAY(VARCHAR)))"""
        result = parse(sql)
        expected = {"select": {"value": {"cast": [
            "x",
            {"row": {"name": "y", "type": {"array": {"varchar": {}}}}},
        ]}}}
        self.assertEqual(result, expected)

    def test_issue_62_structuralC(self):
        sql = """SELECT CAST(x AS ARRAY(ARRAY(VARCHAR)))"""
        result = parse(sql)
        expected = {"select": {"value": {"cast": [
            "x",
            {"array": {"array": {"varchar": {}}}},
        ]}}}
        self.assertEqual(result, expected)
