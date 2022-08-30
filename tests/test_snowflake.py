# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase

from mo_sql_parsing import parse, normal_op


class TestSnowflake(TestCase):
    def test_issue_101_create_temp_table(self):
        sql = """CREATE TEMP TABLE foo(a varchar(10))"""
        result = parse(sql)
        expected = {"create table": {
            "columns": {"name": "a", "type": {"varchar": 10}},
            "name": "foo",
            "temporary": True,
        }}
        self.assertEqual(result, expected)

    def test_issue_101_create_transient_table(self):
        sql = """CREATE TRANSIENT TABLE foo(a varchar(10))"""
        result = parse(sql)
        expected = {"create table": {
            "columns": {"name": "a", "type": {"varchar": 10}},
            "name": "foo",
            "transient": True,
        }}
        self.assertEqual(result, expected)

    def test_issue_102_table_functions1(self):
        sql = """
        SELECT seq4()
        FROM TABLE(generator(rowcount => 10))
        """
        result = parse(sql)
        expected = {
            "from": {"table": {"generator": {}, "rowcount": 10}},
            "select": {"value": {"seq4": {}}},
        }
        self.assertEqual(result, expected)

    def test_issue_102_table_functions2(self):
        sql = """
        SELECT uniform(1, 10, random())
        FROM TABLE(generator(rowcount => 5));
        """
        result = parse(sql)
        expected = {
            "from": {"table": {"generator": {}, "rowcount": 5}},
            "select": {"value": {"uniform": [1, 10, {"random": {}}]}},
        }
        self.assertEqual(result, expected)

    def test_issue_102_table_functions3(self):
        sql = """
        SELECT seq4()
        FROM TABLE(generator(rowcount => 10))
        """
        result = parse(sql, calls=normal_op)
        expected = {
            "from": {
                "op": "table",
                "args": [{"op": "generator", "kwargs": {"rowcount": 10},}],
            },
            "select": {"value": {"op": "seq4"}},
        }
        self.assertEqual(result, expected)

    def test_issue_102_table_functions4(self):
        sql = """
        SELECT uniform(1, 10, random())
        FROM TABLE(generator(rowcount => 5));
        """
        result = parse(sql, calls=normal_op)
        expected = {
            "from": {
                "op": "table",
                "args": [{"op": "generator", "kwargs": {"rowcount": 5},}],
            },
            "select": {"value": {"op": "uniform", "args": [1, 10, {"op": "random"}]}},
        }

        self.assertEqual(result, expected)

    def test_issue_102_table_functions5(self):
        sql = """
        SELECT t.index, t.value
        FROM TABLE(split_to_table('a.b.z.d', '.')) as t
        ORDER BY t.value;
        """
        result = parse(sql)
        expected = {
            "from": {
                "name": "t",
                "value": {"table": {"split_to_table": [
                    {"literal": "a.b.z.d"},
                    {"literal": "."},
                ]}},
            },
            "orderby": {"value": "t.value"},
            "select": [{"value": "t.index"}, {"value": "t.value"}],
        }
        self.assertEqual(result, expected)

    def test_issue_102_within_group(self):
        sql = """
        SELECT listagg(name, ', ' ) WITHIN GROUP (ORDER BY name) AS names
        FROM names_table
        """
        result = parse(sql)
        expected = {
            "from": "names_table",
            "select": {
                "name": "names",
                "value": {"listagg": ["name", {"literal": ", "}]},
                "within": {"orderby": {"value": "name"}},
            },
        }
        self.assertEqual(result, expected)

    def test_issue_105_multiline_strings(self):
        sql = """SELECT 'one
            two
            three'
            FROM my_table"""
        result = parse(sql)
        expected = {
            "from": "my_table",
            "select": {"value": {"literal": "one\n            two\n            three"}},
        }
        self.assertEqual(result, expected)

    def test_issue_104_character_varying1(self):
        sql = """CREATE TABLE foo(a CHARACTER(5))"""
        result = parse(sql)
        expected = {"create table": {
            "columns": {"name": "a", "type": {"character": 5}},
            "name": "foo",
        }}
        self.assertEqual(result, expected)

    def test_issue_104_character_varying2(self):
        sql = """CREATE TABLE foo(a CHARACTER VARYING(5))"""
        result = parse(sql)
        expected = {"create table": {
            "columns": {"name": "a", "type": {"character_varying": 5}},
            "name": "foo",
        }}
        self.assertEqual(result, expected)

    def test_issue_106_index_column_name1(self):
        sql = """SELECT index FROM my_table;"""
        result = parse(sql)
        expected = {"from": "my_table", "select": {"value": "index"}}
        self.assertEqual(result, expected)

    def test_issue_106_index_column_name2(self):
        sql = """CREATE TABLE my_table(index INTEGER);"""
        result = parse(sql)
        expected = {"create table": {
            "columns": {"name": "index", "type": {"integer": {}}},
            "name": "my_table",
        }}
        self.assertEqual(result, expected)

    def test_issue_107_lateral_function(self):
        sql = """SELECT emp.employee_id, emp.last_name, value AS project_name
        FROM employees AS emp, LATERAL flatten(input => emp.project_names) AS proj_names
        ORDER BY employee_id;"""
        result = parse(sql)
        expected = {
            "from": [
                {"name": "emp", "value": "employees"},
                {"lateral": {
                    "name": "proj_names",
                    "value": {"flatten": {}, "input": "emp.project_names"},
                }},
            ],
            "orderby": {"value": "employee_id"},
            "select": [
                {"value": "emp.employee_id"},
                {"value": "emp.last_name"},
                {"name": "project_name", "value": "value"},
            ],
        }
        self.assertEqual(result, expected)

    def test_issue_108_colon1(self):
        sql = """SELECT src:dealership FROM car_sales"""
        result = parse(sql)
        expected = {
            "from": "car_sales",
            "select": {"value": {"get": ["src", {"literal": "dealership"}]}},
        }
        self.assertEqual(result, expected)

    def test_issue_108_colon2(self):
        sql = """SELECT src:salesperson.name FROM car_sales"""
        result = parse(sql)
        expected = {
            "from": "car_sales",
            "select": {"value": {"get": [
                "src",
                {"literal": "salesperson"},
                {"literal": "name"},
            ]}},
        }
        self.assertEqual(result, expected)

    def test_issue_108_colon3(self):
        sql = """SELECT src:['salesperson']['name'] FROM car_sales"""
        result = parse(sql)
        expected = {
            "from": "car_sales",
            "select": {"value": {"get": [
                "src",
                {"literal": "salesperson"},
                {"literal": "name"},
            ]}},
        }
        self.assertEqual(result, expected)

    def test_issue_110_double_quote(self):
        sql = """SELECT REPLACE(foo, '"', '') AS bar FROM my_table"""
        result = parse(sql)
        expected = {
            "from": "my_table",
            "select": {
                "name": "bar",
                "value": {"replace": ["foo", {"literal": '"'}, {"literal": ""}]},
            },
        }
        self.assertEqual(result, expected)

    def test_issue_109_qualify1(self):
        sql = """SELECT id, row_number() OVER (PARTITION BY id ORDER BY id) AS row_num
        FROM my_table
        QUALIFY row_num = 1"""
        result = parse(sql)
        expected = {
            "from": "my_table",
            "qualify": {"eq": ["row_num", 1]},
            "select": [
                {"value": "id"},
                {
                    "name": "row_num",
                    "over": {"orderby": {"value": "id"}, "partitionby": "id"},
                    "value": {"row_number": {}},
                },
            ],
        }

        self.assertEqual(result, expected)

    def test_issue_109_qualify2(self):
        sql = """SELECT id, names
        FROM my_table
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1"""
        result = parse(sql)
        expected = {
            "from": "my_table",
            "qualify": {"eq": [
                {
                    "over": {"orderby": {"value": "id"}, "partitionby": "id"},
                    "value": {"row_number": {}},
                },
                1,
            ]},
            "select": [{"value": "id"}, {"value": "names"}],
        }
        self.assertEqual(result, expected)

    def test_issue_112_qualify(self):
        sql = """SELECT 
            a
        FROM 
            a
        QUALIFY
            ROW_NUMBER() OVER
            (PARTITION BY ssmu.cak, ssmu.rsd  ORDER BY created_at DESC) = 1"""
        result = parse(sql)
        expected = {
            "from": "a",
            "qualify": {"eq": [
                {
                    "over": {
                        "orderby": {"sort": "desc", "value": "created_at"},
                        "partitionby": ["ssmu.cak", "ssmu.rsd"],
                    },
                    "value": {"row_number": {}},
                },
                1,
            ]},
            "select": {"value": "a"},
        }
        self.assertEqual(result, expected)

    def test_issue_101_ilike(self):
        sql = """SELECT * 
        FROM my_table 
        WHERE subject ILIKE '%j%do%'"""
        result = parse(sql)
        expected = {
            "from": "my_table",
            "select": "*",
            "where": {"ilike": ["subject", {"literal": "%j%do%"}]},
        }
        self.assertEqual(result, expected)

    def test_issue_113_dash_in_identifier(self):
        sql = """SELECT SUM(a-b) AS diff
        FROM my_table"""
        result = parse(sql)
        expected = {
            "from": "my_table",
            "select": {"name": "diff", "value": {"sum": {"sub": ["a", "b"]}}},
        }
        self.assertEqual(result, expected)

    def test_issue_114_pivot(self):
        sql = """SELECT *
          FROM (SELECT * FROM monthly_sales_table) monthly_sales
            PIVOT(SUM(amount) FOR month IN ('JAN', 'FEB', 'MAR', 'APR')) AS p
        """
        result = parse(sql)
        expected = {
            "from": [
                {
                    "name": "monthly_sales",
                    "value": {"select": "*", "from": "monthly_sales_table"},
                },
                {
                    "pivot": {
                        "name": "p",
                        "aggregate": {"sum": "amount"},
                        "for": "month",
                        "in": [
                            {"literal": "JAN"},
                            {"literal": "FEB"},
                            {"literal": "MAR"},
                            {"literal": "APR"},
                        ],
                    },
                },
            ],
            "select": "*",
        }

        self.assertEqual(result, expected)

    def test_unpivot(self):
        sql = """SELECT * FROM monthly_sales
        UNPIVOT(sales FOR month IN (jan, feb, mar, april))
        ORDER BY empid;
        """
        result = parse(sql)
        expected = {
            "from": [
                "monthly_sales",
                {"unpivot": {
                    "value": "sales",
                    "for": "month",
                    "in": {"value": ["jan", "feb", "mar", "april"]},
                }},
            ],
            "orderby": {"value": "empid"},
            "select": "*",
        }

        self.assertEqual(result, expected)

