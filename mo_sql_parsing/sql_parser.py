# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_parsing.whitespaces import NO_WHITESPACE, Whitespace

from mo_sql_parsing import utils
from mo_sql_parsing.keywords import *
from mo_sql_parsing.types import get_column_type, time_functions
from mo_sql_parsing.utils import *
from mo_sql_parsing.windows import window


def no_dashes(tokens, start, string):
    if "-" in tokens[0]:
        index = tokens[0].find("-")
        raise ParseException(
            tokens.type,
            start + index + 1,  # +1 TO ENSURE THIS MESSAGE HAS PRIORITY
            string,
            """Ambiguity: Use backticks (``) around identifiers with dashes, or add space around subtraction operator.""",
        )


digit = Char("0123456789")
simple_ident = (
    Char(FIRST_IDENT_CHAR)
    + (Regex("(?<=[^ 0-9])\\-(?=[^ 0-9])") | Char(IDENT_CHAR))[...]
)
simple_ident = Regex(simple_ident.__regex__()[1]) / no_dashes


def common_parser():
    combined_ident = Combine(delimited_list(
        ansi_ident | mysql_backtick_ident | simple_ident, separator=".", combine=True,
    )).set_parser_name("identifier")

    return parser(regex_string | ansi_string, combined_ident)


def mysql_parser():
    utils.emit_warning_for_double_quotes = False

    mysql_string = regex_string | ansi_string | mysql_doublequote_string
    mysql_ident = Combine(delimited_list(
        mysql_backtick_ident | sqlserver_ident | simple_ident,
        separator=".",
        combine=True,
    )).set_parser_name("mysql identifier")

    return parser(mysql_string, mysql_ident)


def sqlserver_parser():
    combined_ident = Combine(delimited_list(
        ansi_ident
        | mysql_backtick_ident
        | sqlserver_ident
        | Word(FIRST_IDENT_CHAR, IDENT_CHAR),
        separator=".",
        combine=True,
    )).set_parser_name("identifier")

    return parser(regex_string | ansi_string, combined_ident, sqlserver=True)


def parser(literal_string, ident, sqlserver=False):

    with Whitespace() as engine:
        rest_of_line = Regex(r"[^\n]*")

        engine.add_ignore(Literal("--") + rest_of_line)
        engine.add_ignore(Literal("#") + rest_of_line)
        engine.add_ignore(Literal("/*") + SkipTo("*/", include=True))

        identifier = ~RESERVED + ident
        function_name = ~FROM + ident

        # EXPRESSIONS
        expression = Forward()
        column_type, column_definition, column_def_references = get_column_type(
            expression, identifier, literal_string
        )

        # CASE
        case = (
            CASE
            + Group(ZeroOrMore(
                (WHEN + expression("when") + THEN + expression("then")) / to_when_call
            ))("case")
            + Optional(ELSE + expression("else"))
            + END
        ) / to_case_call

        switch = (
            CASE
            + expression("value")
            + Group(ZeroOrMore(
                (WHEN + expression("when") + THEN + expression("then")) / to_when_call
            ))("case")
            + Optional(ELSE + expression("else"))
            + END
        ) / to_switch_call

        cast = (
            Group(
                CAST("op") + LB + expression("params") + AS + column_type("params") + RB
            )
            / to_json_call
        )

        # TODO: CAN THIS BE MERGED WITH cast?  DOES THE REGEX OPTIMIZATION BREAK?
        safe_cast = (
            Group(
                SAFE_CAST("op")
                + LB
                + expression("params")
                + AS
                + column_type("params")
                + RB
            )
            / to_json_call
        )

        trim = (
            Group(
                keyword("trim").suppress()
                + LB
                + Optional(
                    (keyword("both") | keyword("trailing") | keyword("leading"))
                    / (lambda t: t[0].lower())
                )("direction")
                + (
                    assign("from", expression)
                    | expression("chars") + Optional(assign("from", expression))
                )
                + RB
            )
            / to_trim_call
        )

        _standard_time_intervals = MatchFirst([
            keyword(d) / (lambda t: durations[t[0].lower()]) for d in durations.keys()
        ]).set_parser_name("duration")("params")

        duration = expression("params") + _standard_time_intervals

        literal_duration = (real_num | int_num)("params") + _standard_time_intervals

        interval = (
            INTERVAL + ("'" + delimited_list(literal_duration) + "'" | duration)
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
            + (_standard_time_intervals | expression("params"))
            + FROM
            + expression("params")
            + RB
        ) / to_json_call

        alias = Optional((
            (
                AS
                + (
                    identifier("name")
                    + Optional(LB + delimited_list(ident("col")) + RB)
                )
                | (
                    identifier("name")
                    + Optional(
                        (LB + delimited_list(ident("col")) + RB)
                        | (AS + delimited_list(identifier("col")))
                    )
                )
            )
            / to_alias
        )("name"))

        named_column = Group(Group(expression)("value") + alias)

        stack = (
            keyword("stack")("op")
            + LB
            + int_num("width")
            + ","
            + delimited_list(expression)("args")
            + RB
        ) / to_stack

        # ARRAY[foo],
        # ARRAY < STRING > [foo, bar], INVALID
        # ARRAY < STRING > [foo, bar],
        create_array = (
            keyword("array")("op")
            + Optional(LT.suppress() + column_type("type") + GT.suppress())
            + (
                LB + delimited_list(Group(expression))("args") + RB
                | (
                    Literal("[")
                    + Optional(delimited_list(Group(expression))("args"))
                    + Literal("]")
                )
            )
        )

        if not sqlserver:
            # SQL SERVER DOES NOT SUPPORT [] FOR ARRAY CONSTRUCTION (USED FOR IDENTIFIERS)
            create_array = (
                Literal("[") + delimited_list(Group(expression))("args") + Literal("]")
                | create_array
            )

        create_array = create_array / to_array

        create_map = (
            keyword("map")
            + Literal("[")
            + expression("keys")
            + ","
            + expression("values")
            + Literal("]")
        ) / to_map

        create_struct = (
            keyword("struct")("op")
            + Optional(
                LT.suppress() + delimited_list(column_type)("types") + GT.suppress()
            )
            + LB
            + delimited_list(Group(
                (expression("value") + alias) / to_select_call
            ))("args")
            + RB
        ) / to_struct

        distinct = (
            DISTINCT("op") + delimited_list(named_column)("params")
        ) / to_json_call

        query = Forward()

        sort_column = (
            expression("value").set_parser_name("sort1")
            + Optional(DESC("sort") | ASC("sort"))
            + Optional(assign("nulls", keyword("first") | keyword("last")))
        )

        call_function = (
            function_name("op")
            + LB
            + Optional(Group(query) | delimited_list(Group(expression)))("params")
            + Optional(
                (keyword("respect") | keyword("ignore"))("nulls")
                + keyword("nulls").suppress()
            )
            + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby"))
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
            | safe_cast
            | distinct
            | trim
            | stack
            | create_array
            | create_map
            | create_struct
            | (LB + Group(query) + RB)
            | (LB + Group(delimited_list(expression)) / to_tuple_call + RB)
            | literal_string
            | hex_num
            | scale_function
            | scale_ident
            | real_num
            | int_num
            | call_function
            | Combine(identifier + Optional(".*"))
        )

        window_clause, over_clause = window(expression, identifier, sort_column)

        expression << (
            (
                Literal("*")
                | infix_notation(
                    compound,
                    [
                        (
                            Literal("[").suppress()
                            + expression
                            + Literal("]").suppress(),
                            1,
                            LEFT_ASSOC,
                            to_offset,
                        ),
                        (
                            Literal(".").suppress() + simple_ident,
                            1,
                            LEFT_ASSOC,
                            to_offset,
                        ),
                        (window_clause, 1, LEFT_ASSOC, to_window_mod),
                        (
                            assign("filter", LB + WHERE + expression + RB),
                            1,
                            LEFT_ASSOC,
                            to_window_mod,
                        ),
                    ]
                    + [
                        (
                            o,
                            1 if o in unary_ops else (3 if isinstance(o, tuple) else 2),
                            unary_ops.get(o, LEFT_ASSOC),
                            to_lambda if o is LAMBDA else to_json_operator,
                        )
                        for o in KNOWN_OPS
                    ],
                )
            )("value").set_parser_name("expression")
        )

        select_column = (
            Group(expression("value") + alias | Literal("*")("value")) / to_select_call
        )

        table_source = Forward()

        join = (
            Group(joins)("op")
            + table_source("join")
            + Optional((ON + expression("on")) | (USING + expression("using")))
            | (
                Group(WINDOW)("op")
                + Group(identifier("name") + AS + over_clause("value"))("join")
            )
        ) / to_join_call

        selection = (
            (
                (SELECT + "*" + EXCEPT.suppress())
                + (LB + delimited_list(select_column)("select_except") + RB)
            )
            | (SELECT + DISTINCT + ON)
            + (LB + delimited_list(select_column)("distinct_on") + RB)
            + delimited_list(select_column)("select")
            | SELECT + DISTINCT + delimited_list(select_column)("select_distinct")
            | (
                SELECT
                + Optional(
                    TOP
                    + expression("value")
                    + Optional(keyword("percent"))("percent")
                    + Optional(WITH + keyword("ties"))("ties")
                )("top")
                / to_top_clause
                + delimited_list(select_column)("select")
            )
        )

        row = (LB + delimited_list(Group(expression)) + RB) / to_row
        values = VALUES + delimited_list(row) / to_values

        unordered_sql = Group(
            values
            | selection
            + Optional(
                (FROM + delimited_list(table_source) + ZeroOrMore(join))("from")
                + Optional(WHERE + expression("where"))
                + Optional(GROUP_BY + delimited_list(Group(named_column))("groupby"))
                + Optional(HAVING + expression("having"))
            )
        )

        with NO_WHITESPACE:

            def mult(tokens):
                amount = tokens["bytes"]
                scale = tokens["scale"].lower()
                return {
                    "bytes": amount
                    * {"b": 1, "k": 1_000, "m": 1_000_000, "g": 1_000_000_000}[scale]
                }

            bytes_constraint = (
                (real_num | int_num)("bytes") + Char("bBkKmMgG")("scale")
            ) / mult

        # https://wiki.postgresql.org/wiki/TABLESAMPLE_Implementation
        # https://docs.snowflake.com/en/sql-reference/constructs/sample.html
        # https://docs.microsoft.com/en-us/sql/t-sql/queries/from-transact-sql?view=sql-server-ver16
        tablesample = (TABLESAMPLE | SAMPLE) + (
            Optional((
                keyword("bernoulli")
                | keyword("row")
                | keyword("system")
                | keyword("block")
            ))("method")
            # / (lambda t: t if t else "bernoulli")
            + LB
            + (
                (
                    keyword("bucket")("op")
                    + int_num("params")
                    + keyword("out of")
                    + int_num("params")
                    + Optional(ON + expression("on"))
                )
                / to_json_call
                | (real_num | int_num)("percent") + keyword("percent")
                | int_num("rows") + keyword("rows")
                | bytes_constraint
                | (real_num | int_num)("percent")
            )
            + RB
            + Optional(assign("repeatable", LB + int_num + RB))
        )("tablesample")

        value_column = (
            TRUE | FALSE | timestamp | literal_string | hex_num | real_num | int_num
        )

        pivot_table = assign(
            "pivot",
            LB
            + expression("aggregate")
            + assign("for", identifier)
            + IN
            + LB
            + delimited_list(value_column)("in")
            + RB
            + RB,
        )

        # <pivoted_table> ::=
        #     table_source PIVOT <pivot_clause> [ [ AS ] table_alias ]
        #
        # <pivot_clause> ::=
        #         ( aggregate_function ( value_column [ [ , ]...n ])
        #         FOR pivot_column
        #         IN ( <column_list> )
        #     )
        #
        # <unpivoted_table> ::=
        #     table_source UNPIVOT <unpivot_clause> [ [ AS ] table_alias ]
        #
        # <unpivot_clause> ::=
        #     ( value_column FOR pivot_column IN ( <column_list> ) )
        table_source << Group(
            ((LB + query + RB) | stack | call_function | identifier)("value")
            + MatchAll([
                Optional(flag("with ordinality")),
                Optional(WITH + LB + keyword("nolock")("hint") + RB),
                Optional(tablesample),
                Optional(pivot_table),
                alias,
            ])
        ) / to_table

        rows = Optional(keyword("row") | keyword("rows"))
        limit = (
            Optional(assign("offset", expression) + rows)
            & Optional(
                FETCH
                + Optional(keyword("first") | keyword("next"))
                + expression("fetch")
                + rows
                + Optional(keyword("only"))
            )
            & Optional(assign("limit", expression))
        )

        ordered_sql = (
            (
                (unordered_sql | (LB + query + RB))
                + ZeroOrMore(
                    Group(
                        (UNION | INTERSECT | EXCEPT | MINUS) + Optional(ALL | DISTINCT)
                    )("op")
                    + (unordered_sql | (LB + query + RB))
                )
            )("union")
            + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby"))
            + limit
            #  def __init__(self, expr, start, string, msg="", cause=None):
            + Optional(
                (UNION | INTERSECT | EXCEPT | MINUS) / bad_operator_on_ordered_sql
            )
        ) / to_union_call

        with_expr = delimited_list(Group(
            (
                (identifier("name") + Optional(LB + delimited_list(ident("col")) + RB))
                / to_alias
            )("name")
            + (AS + LB + (query | expression)("value") + RB)
        ))

        query << (
            Optional(assign("with recursive", with_expr) | assign("with", with_expr))
            + Group(ordered_sql)("query")
        ) / to_query

        #####################################################################
        # DML STATEMENTS
        #####################################################################

        # MySQL's index_type := Using + ( "BTREE" | "HASH" )
        index_type = Optional(assign("using", ident("index_type")))

        index_column_names = LB + delimited_list(identifier("columns")) + RB

        column_def_delete = assign(
            "on delete",
            (keyword("cascade") | keyword("set null") | keyword("set default")),
        )

        table_def_foreign_key = FOREIGN_KEY + Optional(
            Optional(identifier("index_name"))
            + index_column_names
            + column_def_references
            + Optional(column_def_delete)
        )

        index_options = ZeroOrMore(identifier)("table_constraint_options")

        table_constraint_definition = Optional(CONSTRAINT + identifier("name")) + (
            assign("primary key", index_type + index_column_names + index_options)
            | (
                Optional(flag("unique"))
                + Optional(INDEX | KEY)
                + Optional(identifier("name"))
                + index_type
                + index_column_names
                + index_options
            )("index")
            | assign("check", LB + expression + RB)
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
            + identifier("name")
            + Optional(LB + delimited_list(table_element) + RB)
            + ZeroOrMore(
                assign("engine", EQ + identifier)
                | assign("collate", EQ + identifier)
                | assign("auto_increment", EQ + int_num)
                | assign("comment", EQ + literal_string)
                | assign("default character set", EQ + identifier)
                | assign("default charset", EQ + identifier)
            )
            + Optional(AS.suppress() + infix_notation(query, [])("query"))
        )("create table")

        create_view = (
            keyword("create")
            + Optional(keyword("or") + flag("replace"))
            + Optional(flag("temporary"))
            + VIEW.suppress()
            + Optional((keyword("if not exists") / (lambda: False))("replace"))
            + identifier("name")
            + AS
            + query("query")
        )("create view")

        #  CREATE INDEX a ON u USING btree (e);
        create_index = (
            keyword("create index")
            + Optional(keyword("or") + flag("replace"))(INDEX | KEY)
            + Optional((keyword("if not exists") / (lambda: False))("replace"))
            + identifier("name")
            + ON
            + identifier("table")
            + index_type
            + index_column_names
            + index_options
        )("create index")

        cache_options = Optional((
            keyword("options").suppress()
            + LB
            + Dict(delimited_list(Group(
                literal_string / (lambda tokens: tokens[0]["literal"])
                + Optional(EQ)
                + identifier
            )))
            + RB
        )("options"))

        create_cache = (
            keyword("cache").suppress()
            + Optional(flag("lazy"))
            + TABLE
            + identifier("name")
            + cache_options
            + Optional(AS + query("query"))
        )("cache")

        drop_table = (
            keyword("drop table") + Optional(flag("if exists")) + identifier("table")
        )("drop")

        drop_view = (
            keyword("drop view") + Optional(flag("if exists")) + identifier("view")
        )("drop")

        drop_index = (
            keyword("drop index") + Optional(flag("if exists")) + identifier("index")
        )("drop")

        returning = Optional(delimited_list(select_column)("returning"))

        insert = (
            keyword("insert").suppress()
            + (
                flag("overwrite") + keyword("table").suppress()
                | keyword("into").suppress() + Optional(keyword("table").suppress())
            )
            + identifier("table")
            + Optional(LB + delimited_list(identifier)("columns") + RB)
            + Optional(flag("if exists"))
            + (values | query)("query")
            + returning
        ) / to_insert_call

        update = (
            keyword("update")("op")
            + identifier("params")
            + assign("set", Dict(delimited_list(Group(identifier + EQ + expression))))
            + Optional(assign("where", expression))
            + returning
        ) / to_json_call

        delete = (
            keyword("delete")("op")
            + keyword("from").suppress()
            + identifier("params")
            + Optional(assign("where", expression))
            + returning
        ) / to_json_call

        set_parser_names()

        return (
            query
            | (insert | update | delete)
            | (create_table | create_view | create_cache | create_index)
            | (drop_table | drop_view | drop_index)
        ).finalize()
