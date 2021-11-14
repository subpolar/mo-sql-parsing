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
        ansi_ident | mysql_backtick_ident | sqlserver_ident | Word(IDENT_CHAR),
        separator=".",
        combine=True,
    )).set_parser_name("identifier")

    return parser(ansi_string, combined_ident)


def mysql_parser():
    mysql_string = ansi_string | mysql_doublequote_string
    mysql_ident = Combine(delimited_list(
        mysql_backtick_ident | sqlserver_ident | Word(IDENT_CHAR),
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

        # SWITCH
        switch = (
            CASE
            + expr("value")
            + Group(ZeroOrMore(
                (WHEN + expr("when") + THEN + expr("then")) / to_when_call
            ))("case")
            + Optional(ELSE + expr("else"))
            + END
        ) / to_switch_call

        # CAST
        cast = (
            Group(CAST("op") + LB + expr("params") + AS + known_types("params") + RB)
            / to_json_call
        )

        # TRIM
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

        named_column = Group(
            Group(expr)("value") + Optional(Optional(AS) + Group(var_name))("name")
        )

        stack = (
            keyword("stack")("op")
            + LB
            + int_num("width")
            + ","
            + delimited_list(expr)("args")
            + RB
        ) / to_stack
        array = (
            keyword("array")("op") + LB + delimited_list(Group(expr))("args") + RB
        ) / to_array
        map = (
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
            + Optional((keyword("ignore nulls") / (lambda: True))("ignore_nulls"))
            + RB
        ) / to_json_call

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
            | array
            | map
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
                            to_json_operator,
                        )
                        for o in KNOWN_OPS
                    ],
                ).set_parser_name("expression")
            )("value")
            + Optional(window(expr, sort_column))
        ) / to_expression_call

        alias = (
            Group(var_name) + Optional(LB + delimited_list(ident("col")) + RB)
        )("name").set_parser_name("alias") / to_alias

        select_column = (
            Group(
                Group(expr).set_parser_name("expression1")("value")
                + Optional(Optional(AS) + alias)
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
            )("op")
            + Group(table_source)("join")
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

        unordered_sql = values | Group(
            selection
            + Optional(
                (
                    FROM + delimited_list(Group(table_source)) + ZeroOrMore(join)
                )("from")
                + Optional(WHERE + expr("where"))
                + Optional(
                    GROUP_BY + delimited_list(Group(named_column))("groupby")
                )
                + Optional(HAVING + expr("having"))
            )
        ).set_parser_name("unordered sql")

        tablesample = (
            keyword("tablesample").suppress()
            + LB
            + (
                (
                    keyword("bucket")("op")
                    + int_num("params")
                    + keyword("out of")
                    + int_num("params")
                    + Optional(ON + expr("on"))
                )
                / to_json_call
                | real_num("percent") + keyword("percent")
                | int_num("rows") + keyword("rows")
                | real_num("bytes") + Char("bBkKmMgG")
            )
            + RB
        )("tablesample")

        table_source << (
            ((LB + query + RB) | call_function | var_name)("value")
            + Optional(tablesample)
            + Optional(Optional(AS) + alias)
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
            (
                Optional(
                    (
                        WITH
                        + RECURSIVE
                        + alias("name")
                        + AS
                        + LB
                        + (query | expr)("value")
                        + RB
                    )("with recursive")
                    | (
                        WITH
                        + delimited_list(Group(
                            var_name("name") + AS + LB + (query | expr)("value") + RB
                        ))
                    )("with")
                )
                + Group(ordered_sql)("query")
            )
            / to_query
        )

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

        column_def_comment = keyword("comment").suppress() + literal_string("comment")

        column_def_identity = (
            keyword("generated").suppress()
            + (
                (keyword("always") | keyword("by default")) / (lambda: "by_default")
            )("generated")
            + keyword("as identity").suppress()
            + Optional(keyword("start with").suppress() + int_num("start_with"))
            + Optional(keyword("increment by").suppress() + int_num("increment_by"))
        )

        column_def_delete = keyword("on delete").suppress() + (
            keyword("cascade") | keyword("set null") | keyword("set default")
        )("on_delete")

        column_type << (
            struct_type
            | array_type
            | Group(ident("op") + Optional(LB + delimited_list(int_num)("params") + RB))
            / to_json_call
        )

        column_def_references = (
            REFERENCES
            + var_name("table")
            + LB
            + delimited_list(var_name)("columns")
            + RB
        )("references")

        collate = (
            keyword("collate").suppress() + Optional(Char("=").suppress()) + var_name
        )("collate")

        column_def_check = keyword("check").suppress() + LB + expr + RB
        column_def_default = keyword("default").suppress() + expr("default")

        column_options = ZeroOrMore(
            ((NOT + NULL) / (lambda: False))("nullable")
            | (NULL / (lambda t: True))("nullable")
            | (keyword("unique") / (lambda: True))("unique")
            | (keyword("auto_increment") / (lambda: True))("auto_increment")
            | column_def_comment
            | collate
            | (PRIMARY_KEY / (lambda: True))("primary key")
            | column_def_identity("identity")
            | column_def_references
            | column_def_check("check")
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
            (
                PRIMARY_KEY + index_type + index_column_names + index_options
            )("primary_key")
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
            | column_def_check("check")
            | table_def_foreign_key("foreign_key")
        )

        table_element = (
            column_definition("columns") | table_constraint_definition("constraint")
        )

        table_def_engine = (
            keyword("engine").suppress() + Char("=").suppress() + var_name("engine")
        )
        table_def_collate = (
            keyword("collate").suppress() + Char("=").suppress() + var_name("collate")
        )
        table_def_auto_increment = (
            keyword("auto_increment").suppress()
            + Char("=").suppress()
            + int_num("auto_increment")
        )
        table_def_comment = (
            keyword("comment").suppress()
            + Char("=").suppress()
            + literal_string("comment")
        )
        table_def_char_set = (
            keyword("default character set").suppress()
            + Char("=").suppress()
            + var_name("default character set")
        )

        create_table = (
            keyword("create")
            + Optional((keyword("or replace") / (lambda: True))("replace"))
            + Optional((keyword("temporary") / (lambda: True))("temporary"))
            + TABLE
            + Optional((keyword("if not exists") / (lambda: False))("replace"))
            + var_name("name")
            + Optional(LB + delimited_list(table_element) + RB)
            + ZeroOrMore(
                table_def_engine
                | table_def_collate
                | table_def_auto_increment
                | table_def_comment
                | table_def_char_set
            )
            + Optional(AS.suppress() + infix_notation(query, [])("query"))
        )("create table")

        create_view = (
            keyword("create")
            + Optional((keyword("or replace") / (lambda: True))("replace"))
            + Optional((keyword("temporary") / (lambda: True))("temporary"))
            + keyword("view").suppress()
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
                + Optional(Char("=").suppress())
                + var_name
            )))
            + RB
        )("options"))

        create_cache = (
            keyword("cache").suppress()
            + Optional((keyword("lazy") / (lambda: True))("lazy"))
            + TABLE
            + var_name("name")
            + cache_options
            + Optional(AS + query("query"))
        )("cache")

        drop_table = (
            keyword("drop table")
            + Optional((keyword("if exists") / (lambda: True))("if exists"))
            + var_name("table")
        )("drop")

        drop_view = (
            keyword("drop view")
            + Optional((keyword("if exists") / (lambda: True))("if exists"))
            + var_name("view")
        )("drop")

        insert = (
            keyword("insert")("op")
            + (
                (keyword("overwrite") / (lambda: True))("overwrite")
                | keyword("into").suppress()
            )
            + keyword("table").suppress()
            + var_name("params")
            + Optional((keyword("if exists") / (lambda: True))("if exists"))
            + (values("query") | query("query"))
        ) / to_json_call

        update = (
            keyword("update")("op")
            + var_name("params")
            + keyword("set").suppress()
            + delimited_list(Group(
                var_name("name") + Char("=").suppress() + expr("value")
            ))("set")
            + Optional(WHERE + expr("where"))
        ) / to_json_call

        delete = (
            keyword("delete")("op")
            + keyword("from").suppress()
            + var_name("params")
            + Optional(WHERE + expr("where"))
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
