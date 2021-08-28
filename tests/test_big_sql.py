# encoding: utf-8
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase

from mo_json import value2json

from mo_sql_parsing import parse

try:
    from tests.util import assertRaises
except ImportError:
    from .util import assertRaises  # RELATIVE IMPORT SO WE CAN RUN IN pyLibrary


class TestBigSql(TestCase):
    def test_issue_21_many_with_clauses(self):
        sql = """
            WITH ssr
            AS (
                SELECT s_store_id
                    ,sum(sales_price) AS sales
                    ,sum(profit) AS profit
                    ,sum(return_amt) AS
                RETURNS
                    ,sum(net_loss) AS profit_loss
                FROM (
                    SELECT ss_store_sk AS store_sk
                        ,ss_sold_date_sk AS date_sk
                        ,ss_ext_sales_price AS sales_price
                        ,ss_net_profit AS profit
                        ,cast(0 AS DECIMAL(7, 2)) AS return_amt
                        ,cast(0 AS DECIMAL(7, 2)) AS net_loss
                    FROM store_sales
                    
                    UNION ALL
                    
                    SELECT sr_store_sk AS store_sk
                        ,sr_returned_date_sk AS date_sk
                        ,cast(0 AS DECIMAL(7, 2)) AS sales_price
                        ,cast(0 AS DECIMAL(7, 2)) AS profit
                        ,sr_return_amt AS return_amt
                        ,sr_net_loss AS net_loss
                    FROM store_returns
                    ) salesreturns
                    ,date_dim
                    ,store
                WHERE date_sk = d_date_sk
                    AND d_date BETWEEN cast('1998-08-18' AS DATE)
                        AND dateadd(day, 14, cast('1998-08-18' AS DATE))
                    AND store_sk = s_store_sk
                GROUP BY s_store_id
                )
                ,csr
            AS (
                SELECT cp_catalog_page_id
                    ,sum(sales_price) AS sales
                    ,sum(profit) AS profit
                    ,sum(return_amt) AS
                RETURNS
                    ,sum(net_loss) AS profit_loss
                FROM (
                    SELECT cs_catalog_page_sk AS page_sk
                        ,cs_sold_date_sk AS date_sk
                        ,cs_ext_sales_price AS sales_price
                        ,cs_net_profit AS profit
                        ,cast(0 AS DECIMAL(7, 2)) AS return_amt
                        ,cast(0 AS DECIMAL(7, 2)) AS net_loss
                    FROM catalog_sales
                    
                    UNION ALL
                    
                    SELECT cr_catalog_page_sk AS page_sk
                        ,cr_returned_date_sk AS date_sk
                        ,cast(0 AS DECIMAL(7, 2)) AS sales_price
                        ,cast(0 AS DECIMAL(7, 2)) AS profit
                        ,cr_return_amount AS return_amt
                        ,cr_net_loss AS net_loss
                    FROM catalog_returns
                    ) salesreturns
                    ,date_dim
                    ,catalog_page
                WHERE date_sk = d_date_sk
                    AND d_date BETWEEN cast('1998-08-18' AS DATE)
                        AND dateadd(day, 14, cast('1998-08-18' AS DATE))
                    AND page_sk = cp_catalog_page_sk
                GROUP BY cp_catalog_page_id
                )
                ,wsr
            AS (
                SELECT web_site_id
                    ,sum(sales_price) AS sales
                    ,sum(profit) AS profit
                    ,sum(return_amt) AS
                RETURNS
                    ,sum(net_loss) AS profit_loss
                FROM (
                    SELECT ws_web_site_sk AS wsr_web_site_sk
                        ,ws_sold_date_sk AS date_sk
                        ,ws_ext_sales_price AS sales_price
                        ,ws_net_profit AS profit
                        ,cast(0 AS DECIMAL(7, 2)) AS return_amt
                        ,cast(0 AS DECIMAL(7, 2)) AS net_loss
                    FROM web_sales
                    
                    UNION ALL
                    
                    SELECT ws_web_site_sk AS wsr_web_site_sk
                        ,wr_returned_date_sk AS date_sk
                        ,cast(0 AS DECIMAL(7, 2)) AS sales_price
                        ,cast(0 AS DECIMAL(7, 2)) AS profit
                        ,wr_return_amt AS return_amt
                        ,wr_net_loss AS net_loss
                    FROM web_returns
                    LEFT OUTER JOIN web_sales ON (
                            wr_item_sk = ws_item_sk
                            AND wr_order_number = ws_order_number
                            )
                    ) salesreturns
                    ,date_dim
                    ,web_site
                WHERE date_sk = d_date_sk
                    AND d_date BETWEEN cast('1998-08-18' AS DATE)
                        AND dateadd(day, 14, cast('1998-08-18' AS DATE))
                    AND wsr_web_site_sk = web_site_sk
                GROUP BY web_site_id
                )
            SELECT channel
                ,id
                ,sum(sales) AS sales
                ,sum(RETURNS) AS
            RETURNS
                ,sum(profit) AS profit
            FROM (
                SELECT 'store channel' AS channel
                    ,'store' || s_store_id AS id
                    ,sales
                    ,
                RETURNS
                    ,(profit - profit_loss) AS profit
                FROM ssr
                
                UNION ALL
                
                SELECT 'catalog channel' AS channel
                    ,'catalog_page' || cp_catalog_page_id AS id
                    ,sales
                    ,
                RETURNS
                    ,(profit - profit_loss) AS profit
                FROM csr
                
                UNION ALL
                
                SELECT 'web channel' AS channel
                    ,'web_site' || web_site_id AS id
                    ,sales
                    ,
                RETURNS
                    ,(profit - profit_loss) AS profit
                FROM wsr
                ) x
            GROUP BY rollup(channel, id)
            ORDER BY channel
                ,id limit 100;    
        """

        result = parse(sql)
        print(value2json(result, pretty=True))
        expected = {
            "from": {
                "name": "x",
                "value": {"union_all": [
                    {
                        "from": "ssr",
                        "select": [
                            {"name": "channel", "value": {"literal": "store channel"}},
                            {
                                "name": "id",
                                "value": {"concat": [
                                    {"literal": "store"},
                                    "s_store_id",
                                ]},
                            },
                            {"value": "sales"},
                            {"value": "RETURNS"},
                            {
                                "name": "profit",
                                "value": {"sub": ["profit", "profit_loss"]},
                            },
                        ],
                    },
                    {
                        "from": "csr",
                        "select": [
                            {
                                "name": "channel",
                                "value": {"literal": "catalog channel"},
                            },
                            {
                                "name": "id",
                                "value": {"concat": [
                                    {"literal": "catalog_page"},
                                    "cp_catalog_page_id",
                                ]},
                            },
                            {"value": "sales"},
                            {"value": "RETURNS"},
                            {
                                "name": "profit",
                                "value": {"sub": ["profit", "profit_loss"]},
                            },
                        ],
                    },
                    {
                        "from": "wsr",
                        "select": [
                            {"name": "channel", "value": {"literal": "web channel"}},
                            {
                                "name": "id",
                                "value": {"concat": [
                                    {"literal": "web_site"},
                                    "web_site_id",
                                ]},
                            },
                            {"value": "sales"},
                            {"value": "RETURNS"},
                            {
                                "name": "profit",
                                "value": {"sub": ["profit", "profit_loss"]},
                            },
                        ],
                    },
                ]},
            },
            "groupby": {"value": {"rollup": ["channel", "id"]}},
            "limit": 100,
            "orderby": [{"value": "channel"}, {"value": "id"}],
            "select": [
                {"value": "channel"},
                {"value": "id"},
                {"name": "sales", "value": {"sum": "sales"}},
                {"name": "RETURNS", "value": {"sum": "RETURNS"}},
                {"name": "profit", "value": {"sum": "profit"}},
            ],
            "with": [
                {
                    "name": "ssr",
                    "value": {
                        "from": [
                            {
                                "name": "salesreturns",
                                "value": {"union_all": [
                                    {
                                        "from": "store_sales",
                                        "select": [
                                            {
                                                "name": "store_sk",
                                                "value": "ss_store_sk",
                                            },
                                            {
                                                "name": "date_sk",
                                                "value": "ss_sold_date_sk",
                                            },
                                            {
                                                "name": "sales_price",
                                                "value": "ss_ext_sales_price",
                                            },
                                            {
                                                "name": "profit",
                                                "value": "ss_net_profit",
                                            },
                                            {
                                                "name": "return_amt",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                            {
                                                "name": "net_loss",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                        ],
                                    },
                                    {
                                        "from": "store_returns",
                                        "select": [
                                            {
                                                "name": "store_sk",
                                                "value": "sr_store_sk",
                                            },
                                            {
                                                "name": "date_sk",
                                                "value": "sr_returned_date_sk",
                                            },
                                            {
                                                "name": "sales_price",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                            {
                                                "name": "profit",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                            {
                                                "name": "return_amt",
                                                "value": "sr_return_amt",
                                            },
                                            {
                                                "name": "net_loss",
                                                "value": "sr_net_loss",
                                            },
                                        ],
                                    },
                                ]},
                            },
                            "date_dim",
                            "store",
                        ],
                        "groupby": {"value": "s_store_id"},
                        "select": [
                            {"value": "s_store_id"},
                            {"name": "sales", "value": {"sum": "sales_price"}},
                            {"name": "profit", "value": {"sum": "profit"}},
                            {"name": "RETURNS", "value": {"sum": "return_amt"}},
                            {"name": "profit_loss", "value": {"sum": "net_loss"}},
                        ],
                        "where": {"and": [
                            {"eq": ["date_sk", "d_date_sk"]},
                            {"between": [
                                "d_date",
                                {"cast": [{"literal": "1998-08-18"}, {"date": {}}]},
                                {"dateadd": [
                                    "day",
                                    14,
                                    {"cast": [{"literal": "1998-08-18"}, {"date": {}}]},
                                ]},
                            ]},
                            {"eq": ["store_sk", "s_store_sk"]},
                        ]},
                    },
                },
                {
                    "name": "csr",
                    "value": {
                        "from": [
                            {
                                "name": "salesreturns",
                                "value": {"union_all": [
                                    {
                                        "from": "catalog_sales",
                                        "select": [
                                            {
                                                "name": "page_sk",
                                                "value": "cs_catalog_page_sk",
                                            },
                                            {
                                                "name": "date_sk",
                                                "value": "cs_sold_date_sk",
                                            },
                                            {
                                                "name": "sales_price",
                                                "value": "cs_ext_sales_price",
                                            },
                                            {
                                                "name": "profit",
                                                "value": "cs_net_profit",
                                            },
                                            {
                                                "name": "return_amt",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                            {
                                                "name": "net_loss",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                        ],
                                    },
                                    {
                                        "from": "catalog_returns",
                                        "select": [
                                            {
                                                "name": "page_sk",
                                                "value": "cr_catalog_page_sk",
                                            },
                                            {
                                                "name": "date_sk",
                                                "value": "cr_returned_date_sk",
                                            },
                                            {
                                                "name": "sales_price",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                            {
                                                "name": "profit",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                            {
                                                "name": "return_amt",
                                                "value": "cr_return_amount",
                                            },
                                            {
                                                "name": "net_loss",
                                                "value": "cr_net_loss",
                                            },
                                        ],
                                    },
                                ]},
                            },
                            "date_dim",
                            "catalog_page",
                        ],
                        "groupby": {"value": "cp_catalog_page_id"},
                        "select": [
                            {"value": "cp_catalog_page_id"},
                            {"name": "sales", "value": {"sum": "sales_price"}},
                            {"name": "profit", "value": {"sum": "profit"}},
                            {"name": "RETURNS", "value": {"sum": "return_amt"}},
                            {"name": "profit_loss", "value": {"sum": "net_loss"}},
                        ],
                        "where": {"and": [
                            {"eq": ["date_sk", "d_date_sk"]},
                            {"between": [
                                "d_date",
                                {"cast": [{"literal": "1998-08-18"}, {"date": {}}]},
                                {"dateadd": [
                                    "day",
                                    14,
                                    {"cast": [{"literal": "1998-08-18"}, {"date": {}}]},
                                ]},
                            ]},
                            {"eq": ["page_sk", "cp_catalog_page_sk"]},
                        ]},
                    },
                },
                {
                    "name": "wsr",
                    "value": {
                        "from": [
                            {
                                "name": "salesreturns",
                                "value": {"union_all": [
                                    {
                                        "from": "web_sales",
                                        "select": [
                                            {
                                                "name": "wsr_web_site_sk",
                                                "value": "ws_web_site_sk",
                                            },
                                            {
                                                "name": "date_sk",
                                                "value": "ws_sold_date_sk",
                                            },
                                            {
                                                "name": "sales_price",
                                                "value": "ws_ext_sales_price",
                                            },
                                            {
                                                "name": "profit",
                                                "value": "ws_net_profit",
                                            },
                                            {
                                                "name": "return_amt",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                            {
                                                "name": "net_loss",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                        ],
                                    },
                                    {
                                        "from": [
                                            "web_returns",
                                            {
                                                "left outer join": "web_sales",
                                                "on": {"and": [
                                                    {"eq": [
                                                        "wr_item_sk",
                                                        "ws_item_sk",
                                                    ]},
                                                    {"eq": [
                                                        "wr_order_number",
                                                        "ws_order_number",
                                                    ]},
                                                ]},
                                            },
                                        ],
                                        "select": [
                                            {
                                                "name": "wsr_web_site_sk",
                                                "value": "ws_web_site_sk",
                                            },
                                            {
                                                "name": "date_sk",
                                                "value": "wr_returned_date_sk",
                                            },
                                            {
                                                "name": "sales_price",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                            {
                                                "name": "profit",
                                                "value": {"cast": [
                                                    0,
                                                    {"decimal": [7, 2]},
                                                ]},
                                            },
                                            {
                                                "name": "return_amt",
                                                "value": "wr_return_amt",
                                            },
                                            {
                                                "name": "net_loss",
                                                "value": "wr_net_loss",
                                            },
                                        ],
                                    },
                                ]},
                            },
                            "date_dim",
                            "web_site",
                        ],
                        "groupby": {"value": "web_site_id"},
                        "select": [
                            {"value": "web_site_id"},
                            {"name": "sales", "value": {"sum": "sales_price"}},
                            {"name": "profit", "value": {"sum": "profit"}},
                            {"name": "RETURNS", "value": {"sum": "return_amt"}},
                            {"name": "profit_loss", "value": {"sum": "net_loss"}},
                        ],
                        "where": {"and": [
                            {"eq": ["date_sk", "d_date_sk"]},
                            {"between": [
                                "d_date",
                                {"cast": [{"literal": "1998-08-18"}, {"date": {}}]},
                                {"dateadd": [
                                    "day",
                                    14,
                                    {"cast": [{"literal": "1998-08-18"}, {"date": {}}]},
                                ]},
                            ]},
                            {"eq": ["wsr_web_site_sk", "web_site_sk"]},
                        ]},
                    },
                },
            ],
        }

        self.assertEqual(result, expected)
