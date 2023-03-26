# encoding: utf-8
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase

from mo_sql_parsing import parse_mysql, parse


class TestMySql(TestCase):
    def test_issue_22(self):
        sql = 'SELECT "fred"'
        result = parse_mysql(sql)
        expected = {"select": {"value": {"literal": "fred"}}}
        self.assertEqual(result, expected)

    def test_issue_126(self):
        from mo_sql_parsing import parse_mysql as parse

        result = parse(
            """SELECT algid, item_id, avg(score) as avg_score 
            from (
                select 
                    ds, 
                    algid, 
                    uin, 
                    split(item_result, ":")[0] as item_id, 
                    split(item_result, ": ")[1] as score 
                from (
                    select ds, scorealgid_ as algid, uin_ as uin, item_result 
                    from (
                        select * 
                        from t_dwd_tmp_wxpay_discount_model_score_hour 
                        where ds >= 2022080300 and ds <= 2022080323
                    )day_tbl 
                    LATERAL VIEW explode(split(scoreresult_, "\\ | ")) temp AS item_result
                ) t
            ) tbl 
            group by algid, item_id;"""
        )
        expected = {
            "from": {
                "name": "tbl",
                "value": {
                    "from": {
                        "name": "t",
                        "value": {
                            "from": [
                                {
                                    "name": "day_tbl",
                                    "value": {
                                        "from": "t_dwd_tmp_wxpay_discount_model_score_hour",
                                        "select": "*",
                                        "where": {"and": [{"gte": ["ds", 2022080300]}, {"lte": ["ds", 2022080323]}]},
                                    },
                                },
                                {"lateral view": {
                                    "name": {"temp": "item_result"},
                                    "value": {"explode": {"split": ["scoreresult_", {"literal": "\\ | "}]}},
                                }},
                            ],
                            "select": [
                                {"value": "ds"},
                                {"name": "algid", "value": "scorealgid_"},
                                {"name": "uin", "value": "uin_"},
                                {"value": "item_result"},
                            ],
                        },
                    },
                    "select": [
                        {"value": "ds"},
                        {"value": "algid"},
                        {"value": "uin"},
                        {"name": "item_id", "value": {"get": [{"split": ["item_result", {"literal": ":"}]}, 0]}},
                        {"name": "score", "value": {"get": [{"split": ["item_result", {"literal": ": "}]}, 1]}},
                    ],
                },
            },
            "groupby": [{"value": "algid"}, {"value": "item_id"}],
            "select": [{"value": "algid"}, {"value": "item_id"}, {"name": "avg_score", "value": {"avg": "score"}}],
        }
        self.assertEqual(result, expected)

    def test_issue_157_describe1(self):
        sql = """Explain format=traditional select * from temp"""
        result = parse(sql)
        expected = {"explain": {"from": "temp", "select": "*"}, "format": "traditional"}
        self.assertEqual(result, expected)

    def test_issue_157_describe2(self):
        sql = """desc format=tree select * from temp"""
        result = parse(sql)
        expected = {"explain": {"from": "temp", "select": "*"}, "format": "tree"}
        self.assertEqual(result, expected)

    def test_issue_157_describe3(self):
        sql = """desc format=json select * from temp"""
        result = parse(sql)
        expected = {"explain": {"from": "temp", "select": "*"}, "format": "json"}
        self.assertEqual(result, expected)

    def test_merge_into(self):
        sql = """
            MERGE INTO TMP_TABLE1 TMP1
            USING TMP_TABLE1 TMP2
            ON TMP1.col1 =TMP2.col1
            AND TMP1.col2=TMP2.col2
            AND TMP1.col3=TMP2.col3
            AND (TMP2.col4 - 1) = TMP1.col4
            WHEN MATCHED THEN
            UPDATE SET ZTAGG_END = TMP2.ZTAGG"""
        result = parse(sql)
        expected = {}
        self.assertEqual(result, expected)

    def test_merge1(self):
        # from https://www.sqlshack.com/understanding-the-sql-merge-statement/
        sql = """
            MERGE TargetProducts AS Target
            USING SourceProducts	AS Source
            ON Source.ProductID = Target.ProductID
            WHEN NOT MATCHED BY Target THEN
                INSERT (ProductID,ProductName, Price) 
                VALUES (Source.ProductID,Source.ProductName, Source.Price);
        """
        result = parse(sql)
        expected = {}
        self.assertEqual(result, expected)

    def test_merge2(self):
        # from https://www.sqlshack.com/understanding-the-sql-merge-statement/
        sql = """
            MERGE TargetProducts AS Target
            USING SourceProducts	AS Source
            ON Source.ProductID = Target.ProductID
            
            -- For Inserts
            WHEN NOT MATCHED BY Target THEN
                INSERT (ProductID,ProductName, Price) 
                VALUES (Source.ProductID,Source.ProductName, Source.Price)
            
            -- For Updates
            WHEN MATCHED THEN UPDATE SET
                Target.ProductName	= Source.ProductName,
                Target.Price		= Source.Price;
        """
        result = parse(sql)
        expected = {}
        self.assertEqual(result, expected)

    def test_merge3(self):
        # from https://www.sqlshack.com/understanding-the-sql-merge-statement/
        sql = """ 
            MERGE TargetProducts AS Target
            USING SourceProducts	AS Source
            ON Source.ProductID = Target.ProductID
                
            -- For Inserts
            WHEN NOT MATCHED BY Target THEN
                INSERT (ProductID,ProductName, Price) 
                VALUES (Source.ProductID,Source.ProductName, Source.Price)
                
            -- For Updates
            WHEN MATCHED THEN UPDATE SET
                Target.ProductName	= Source.ProductName,
                Target.Price		= Source.Price
                
            -- For Deletes
            WHEN NOT MATCHED BY Source THEN
                DELETE;        
        """
        result = parse(sql)
        expected = {}
        self.assertEqual(result, expected)
