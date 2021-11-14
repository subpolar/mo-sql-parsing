# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_parsing.helpers import delimited_list, restOfLine
from mo_parsing.whitespaces import NO_WHITESPACE, Whitespace

from mo_sql_parsing.keywords import *
from mo_sql_parsing.utils import *
from mo_sql_parsing.windows import window


def combined_parser():
    combined_ident = Combine(delimited_list(
        ansi_ident
        | mysql_backtick_ident
        | sqlserver_ident
        | Word(FIRST_IDENT_CHAR, IDENT_CHAR),
        separator=".",
        combine=True,
    )).set_parser_name("identifier")

    return parser(ansi_string, combined_ident)


def mysql_parser():
    mysql_string = ansi_string | mysql_doublequote_string
    mysql_ident = Combine(delimited_list(
        mysql_backtick_ident | sqlserver_ident | Word(FIRST_IDENT_CHAR, IDENT_CHAR),
        separator=".",
        combine=True,
    )).set_parser_name("mysql identifier")

    return parser(mysql_string, mysql_ident)


def parser(literal_string, ident):
    with Whitespace() as engine:
        engine.add_ignore(Literal("--") + restOfLine)
        engine.add_ignore(Literal("#") + restOfLine)

        var_name = ~RESERVED + ident

        # EXPRESSIONS
        column_definition = Forward()
        column_type = Forward()
        expr = Forward()

        # CASE
        case = (
            CASE
            + Group(ZeroOrMore(
                (WHEN + expr("when") + THEN + expr("then")) / to_when_call
            ))("case")
            + Optional(ELSE + expr("else"))
            + END
        ) / to_case_call

        switch = (
            CASE
            + expr("value")
            + Group(ZeroOrMore(
                (WHEN + expr("when") + THEN + expr("then")) / to_when_call
            ))("case")
            + Optional(ELSE + expr("else"))
            + END
        ) / to_switch_call

        cast = (
            Group(CAST("op") + LB + expr("params") + AS + known_types("params") + RB)
            / to_json_call
        )

        trim = (
            Group(
                keyword("trim").suppress()
                + LB
                + expr("chars")
                + Optional(FROM + expr("from"))
                + RB
            ).set_parser_name("trim")
            / to_trim_call
        )

        _standard_time_intervals = MatchFirst([
            keyword(d) / (lambda t: durations[t[0].lower()]) for d in durations.keys()
        ]).set_parser_name("duration")("params")

        duration = (
            real_num | int_num | literal_string
        )("params") + _standard_time_intervals

        interval = (
            INTERVAL + ("'" + delimited_list(duration) + "'" | duration)
        ) / to_interval_call

        timestamp = (
            time_functions("op")
            + (
                literal_string("params")
                | MatchFirst([
                    keyword(t) / (lambda t: t.lower()) for t in times
                ])("params")
            )
        ) / to_json_call

        extract = (
            keyword("extract")("op")
            + LB
            + (_standard_time_intervals | expr("params"))
            + FROM
            + expr("params")
            + RB
        ) / to_json_call

        alias = Optional((
            (
                AS
                + (var_name("name") + Optional(LB + delimited_list(ident("col")) + RB))
                | (var_name("name") + Optional(AS + delimited_list(var_name("col"))))
            )
            / to_alias
        )("name"))

        named_column = Group(Group(expr)("value") + alias)

        stack = (
            keyword("stack")("op")
            + LB
            + int_num("width")
            + ","
            + delimited_list(expr)("args")
            + RB
        ) / to_stack

        create_array = (
            keyword("array")("op") + LB + delimited_list(Group(expr))("args") + RB
        ) / to_array

        create_map = (
            keyword("map") + Char("[") + expr("keys") + "," + expr("values") + Char("]")
        ) / to_map

        distinct = (
            DISTINCT("op") + delimited_list(named_column)("params")
        ) / to_json_call

        query = Forward().set_parser_name("query")

        call_function = (
            ident("op")
            + LB
            + Optional(Group(query) | delimited_list(Group(expr)))("params")
            + Optional(flag("ignore nulls"))
            + RB
        ).set_parser_name("call function") / to_json_call

        with NO_WHITESPACE:

            def scale(tokens):
                return {"mul": [tokens[0], tokens[1]]}

            scale_function = ((real_num | int_num) + call_function) / scale
            scale_ident = ((real_num | int_num) + ident) / scale

        compound = (
            NULL
            | TRUE
            | FALSE
            | NOCASE
            | interval
            | timestamp
            | extract
            | case
            | switch
            | cast
            | distinct
            | trim
            | stack
            | create_array
            | create_map
            | (LB + Group(query) + RB)
            | (LB + Group(delimited_list(expr)) / to_tuple_call + RB)
            | literal_string.set_parser_name("string")
            | hex_num.set_parser_name("hex")
            | scale_function
            | scale_ident
            | real_num.set_parser_name("float")
            | int_num.set_parser_name("int")
            | call_function
            | Combine(var_name + Optional(".*"))
        )

        sort_column = expr("value").set_parser_name("sort1") + Optional(
            DESC("sort") | ASC("sort")
        ) | expr("value").set_parser_name("sort2")

        expr << (
            (
                Literal("*")
                | infix_notation(
                    compound,
                    [(
                        Char("[").suppress() + expr + Char("]").suppress(),
                        1,
                        LEFT_ASSOC,
                        to_offset,
                    )]
                    + [
                        (
                            o,
                            1 if o in unary_ops else (3 if isinstance(o, tuple) else 2),
                            unary_ops[o] if o in unary_ops else LEFT_ASSOC,
                            to_lambda if o is LAMBDA else to_json_operator,
                        )
                        for o in KNOWN_OPS
                    ],
                ).set_parser_name("expression")
            )("value")
            + Optional(window(expr, sort_column))
        ) / to_expression_call

        select_column = (
            Group(
                Group(expr).set_parser_name("expression")("value") + alias
                | Literal("*")("value")
            ).set_parser_name("column")
            / to_select_call
        )

        table_source = Forward()

        join = (
            Group(
                CROSS_JOIN
                | FULL_JOIN
                | FULL_OUTER_JOIN
                | OUTER_JOIN
                | INNER_JOIN
                | JOIN
                | LEFT_JOIN
                | LEFT_OUTER_JOIN
                | LEFT_INNER_JOIN
                | RIGHT_JOIN
                | RIGHT_OUTER_JOIN
                | RIGHT_INNER_JOIN
                | LATERAL_VIEW_OUTER
                | LATERAL_VIEW
            )("op")
            + table_source("join")
            + Optional((ON + expr("on")) | (USING + expr("using")))
        ) / to_join_call

        selection = (
            SELECT
            + DISTINCT
            + ON
            + LB
            + delimited_list(select_column)("distinct_on")
            + RB
            + delimited_list(select_column)("select")
            | SELECT + DISTINCT + delimited_list(select_column)("select_distinct")
            | (
                SELECT
                + Optional(
                    TOP
                    + expr("value")
                    + Optional(keyword("percent"))("percent")
                    + Optional(WITH + keyword("ties"))("ties")
                )("top")
                / to_top_clause
                + delimited_list(select_column)("select")
            )
        )

        row = (LB + delimited_list(Group(expr)) + RB) / to_row
        values = VALUES + delimited_list(row) / to_values

        unordered_sql = Group(
            values
            | selection
            + Optional(
                (FROM + delimited_list(table_source) + ZeroOrMore(join))("from")
                + Optional(WHERE + expr("where"))
                + Optional(GROUP_BY + delimited_list(Group(named_column))("groupby"))
                + Optional(HAVING + expr("having"))
            )
        ).set_parser_name("unordered sql")

        with NO_WHITESPACE:

            def mult(tokens):
                amount = tokens["bytes"]
                scale = tokens["scale"].lower()
                return {
                    "bytes": amount
                    * {"b": 1, "k": 1_000, "m": 1_000_000, "g": 1_000_000_000}[scale]
                }

            ts_bytes = (
                (real_num | int_num)("bytes") + Char("bBkKmMgG")("scale")
            ) / mult

        tablesample = assign(
            "tablesample",
            LB
            + (
                (
                    keyword("bucket")("op")
                    + int_num("params")
                    + keyword("out of")
                    + int_num("params")
                    + Optional(ON + expr("on"))
                )
                / to_json_call
                | (real_num | int_num)("percent") + keyword("percent")
                | int_num("rows") + keyword("rows")
                | ts_bytes
            )
            + RB,
        )

        table_source << Group(
            ((LB + query + RB) | stack | call_function | var_name)("value")
            + Optional(flag("with ordinality"))
            + Optional(tablesample)
            + alias
        ).set_parser_name("table_source") / to_table

        ordered_sql = (
            (
                unordered_sql
                + ZeroOrMore(
                    Group(
                        (UNION | INTERSECT | EXCEPT | MINUS) + Optional(ALL | DISTINCT)
                    )("op")
                    + unordered_sql
                )
            )("union")
            + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby"))
            + Optional(LIMIT + expr("limit"))
            + Optional(OFFSET + expr("offset"))
        ).set_parser_name("ordered sql") / to_union_call

        query << (
            Optional(
                assign(
                    "with recursive",
                    (
                        (
                            var_name("name")
                            + Optional(LB + delimited_list(ident("col")) + RB)
                        )
                        / to_alias
                    )("name")
                    + AS
                    + LB
                    + (query | expr)("value")
                    + RB,
                )
                | assign(
                    "with",
                    delimited_list(Group(
                        var_name("name") + AS + LB + (query | expr)("value") + RB
                    )),
                )
            )
            + Group(ordered_sql)("query")
        ) / to_query

        #####################################################################
        # DML STATEMENTS
        #####################################################################
        struct_type = (
            keyword("struct")("op")
            + Literal("<").suppress()
            + delimited_list(column_definition)("params")
            + Literal(">").suppress()
        ) / to_json_call

        array_type = (
            keyword("array")("op")
            + Literal("<").suppress()
            + delimited_list(column_type)("params")
            + Literal(">").suppress()
        ) / to_json_call

        column_def_comment = assign("comment", literal_string)

        column_def_identity = (
            assign(
                "generated",
                (keyword("always") | keyword("by default") / (lambda: "by_default")),
            )
            + keyword("as identity").suppress()
            + Optional(assign("start with", int_num))
            + Optional(assign("increment by", int_num))
        )

        column_def_delete = assign(
            "on delete",
            (keyword("cascade") | keyword("set null") | keyword("set default")),
        )

        column_type << (
            struct_type
            | array_type
            | Group(ident("op") + Optional(LB + delimited_list(int_num)("params") + RB))
            / to_json_call
        )

        column_def_references = assign(
            "references",
            var_name("table") + LB + delimited_list(var_name)("columns") + RB,
        )

        collate = assign("collate", Optional(EQ) + var_name)

        column_def_check = assign("check", LB + expr + RB)
        column_def_default = assign("default", expr)

        column_options = ZeroOrMore(
            ((NOT + NULL) / (lambda: False))("nullable")
            | (NULL / (lambda t: True))("nullable")
            | flag("unique")
            | flag("auto_increment")
            | column_def_comment
            | collate
            | flag("primary key")
            | column_def_identity("identity")
            | column_def_references
            | column_def_check
            | column_def_default
        ).set_parser_name("column_options")

        column_definition << Group(
            var_name("name") / (lambda t: t[0].lower())
            + column_type("type")
            + column_options
        ).set_parser_name("column_definition")

        # MySQL's index_type := Using + ( "BTREE" | "HASH" )
        index_type = Optional(USING + ident("index_type"))

        index_column_names = LB + delimited_list(var_name("columns")) + RB

        table_def_foreign_key = FOREIGN_KEY + Optional(
            Optional(var_name("index_name"))
            + index_column_names
            + column_def_references
            + Optional(column_def_delete)
        )

        index_options = ZeroOrMore(var_name)("table_constraint_options")

        table_constraint_definition = Optional(CONSTRAINT + var_name("name")) + (
            assign("primary key", index_type + index_column_names + index_options)
            | (
                UNIQUE
                + Optional(INDEX | KEY)
                + Optional(var_name("index_name"))
                + index_type
                + index_column_names
                + index_options
            )("unique")
            | (
                (INDEX | KEY)
                + Optional(var_name("index_name"))
                + index_type
                + index_column_names
                + index_options
            )("index")
            | column_def_check
            | table_def_foreign_key("foreign_key")
        )

        table_element = (
            column_definition("columns") | table_constraint_definition("constraint")
        )

        create_table = (
            keyword("create")
            + Optional(keyword("or") + flag("replace"))
            + Optional(flag("temporary"))
            + TABLE
            + Optional((keyword("if not exists") / (lambda: False))("replace"))
            + var_name("name")
            + Optional(LB + delimited_list(table_element) + RB)
            + ZeroOrMore(
                assign("engine", EQ + var_name)
                | assign("collate", EQ + var_name)
                | assign("auto_increment", EQ + int_num)
                | assign("comment", EQ + literal_string)
                | assign("default character set", EQ + var_name)
            )
            + Optional(AS.suppress() + infix_notation(query, [])("query"))
        )("create table")

        create_view = (
            keyword("create")
            + Optional(keyword("or") + flag("replace"))
            + Optional(flag("temporary"))
            + VIEW.suppress()
            + Optional((keyword("if not exists") / (lambda: False))("replace"))
            + var_name("name")
            + AS
            + query("query")
        )("create view")

        cache_options = Optional((
            keyword("options").suppress()
            + LB
            + Dict(delimited_list(Group(
                literal_string / (lambda tokens: tokens[0]["literal"])
                + Optional(EQ)
                + var_name
            )))
            + RB
        )("options"))

        create_cache = (
            keyword("cache").suppress()
            + Optional(flag("lazy"))
            + TABLE
            + var_name("name")
            + cache_options
            + Optional(AS + query("query"))
        )("cache")

        drop_table = (
            keyword("drop table") + Optional(flag("if exists")) + var_name("table")
        )("drop")

        drop_view = (
            keyword("drop view") + Optional(flag("if exists")) + var_name("view")
        )("drop")

        insert = (
            keyword("insert")("op")
            + (flag("overwrite") | keyword("into").suppress())
            + keyword("table").suppress()
            + var_name("params")
            + Optional(flag("if exists"))
            + (values | query)("query")
        ) / to_json_call

        update = (
            keyword("update")("op")
            + var_name("params")
            + assign("set", Dict(delimited_list(Group(var_name + EQ + expr))))
            + Optional(assign("where", expr))
        ) / to_json_call

        delete = (
            keyword("delete")("op")
            + keyword("from").suppress()
            + var_name("params")
            + Optional(assign("where", expr))
        ) / to_json_call

        return (
            query
            | insert
            | update
            | delete
            | create_table
            | create_view
            | create_cache
            | drop_table
            | drop_view
        ).finalize()
