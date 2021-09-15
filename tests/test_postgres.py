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


class TestPostgres(TestCase):
    def test_issue_15(self):
        sql = """
        SELECT 
            id, 
            create_date AT TIME ZONE 'UTC' as created_at, 
            write_date AT TIME ZONE 'UTC' as updated_at
        FROM sometable;
        """
        result = parse(sql)

        self.assertEqual(
            result,
            {
                "from": "sometable",
                "select": [
                    {"value": "id"},
                    {
                        "name": "created_at",
                        "value": {"": ["create_date", {"literal": "UTC"}]},
                    },
                    {
                        "name": "updated_at",
                        "value": {"": ["write_date", {"literal": "UTC"}]},
                    },
                ],
            },
        )

    def test_issue_20(self):
        sql = """SELECT Status FROM city WHERE Population > 1500 INTERSECT SELECT Status FROM city WHERE Population < 500"""
        result = parse(sql)
        expected = {"intersect": [
            {
                "from": "city",
                "select": {"value": "Status"},
                "where": {"gt": ["Population", 1500]},
            },
            {
                "from": "city",
                "select": {"value": "Status"},
                "where": {"lt": ["Population", 500]},
            },
        ]}
        self.assertEqual(result, expected)

    def test_issue_19a(self):
        # https: // docs.microsoft.com / en - us / sql / t - sql / functions / trim - transact - sql?view = sql - server - ver15
        sql = "select trim(' ' from ' This is a test') from dual"
        result = parse(sql)
        expected = {
            "from": "dual",
            "select": {"value": {
                "trim": {"literal": " This is a test"},
                "characters": {"literal": " "}
            }},
        }
        self.assertEqual(result, expected)

    def test_issue_19b(self):
        sql = "select trim(' testing  ') from dual"
        result = parse(sql)
        expected = {
            "from": "dual",
            "select": {"value": {"trim": {"literal": " testing  "}}},
        }
        self.assertEqual(result, expected)

    def test_except(self):
        sql = """select name from employee
        except
        select 'Alan' from dual
        """
        result = parse(sql)
        expected = {"except": [
            {"from": "employee", "select": {"value": "name"}},
            {"from": "dual", "select": {"value": {"literal": "Alan"}}},
        ]}
        self.assertEqual(result, expected)
