# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from mo_parsing.engine import Engine
from mo_parsing.helpers import delimitedList, restOfLine
from moz_sql_parser.keywords import *
from moz_sql_parser.utils import *
from moz_sql_parser.windows import sortColumn, window

engine = Engine().use()
engine.add_ignore(Literal("--") + restOfLine)
engine.add_ignore(Literal("#") + restOfLine)

# IDENTIFIER
literal_string = Regex(r'\"(\"\"|[^"])*\"').addParseAction(unquote)
mysql_ident = Regex(r"\`(\`\`|[^`])*\`").addParseAction(unquote)
sqlserver_ident = Regex(r"\[(\]\]|[^\]])*\]").addParseAction(unquote)
ident = Combine(
    ~RESERVED
    + (delimitedList(
        Literal("*")
        | literal_string
        | mysql_ident
        | sqlserver_ident
        | Word(IDENT_CHAR),
        separator=".",
        combine=True,
    ))
).set_parser_name("identifier")

# EXPRESSIONS

# CASE
case = (
    CASE
    + Group(ZeroOrMore(
        (WHEN + expr("when") + THEN + expr("then")).addParseAction(to_when_call)
    ))("case")
    + Optional(ELSE + expr("else"))
    + END
).addParseAction(to_case_call)

# SWITCH
switch = (
    CASE
    + expr("value")
    + Group(ZeroOrMore(
        (WHEN + expr("when") + THEN + expr("then")).addParseAction(to_when_call)
    ))("case")
    + Optional(ELSE + expr("else"))
    + END
).addParseAction(to_switch_call)

# CAST
cast = Group(
    CAST("op") + LB + expr("params") + AS + known_types("params") + RB
).addParseAction(to_json_call)

_standard_time_intervals = MatchFirst([
    Keyword(d, caseless=True).addParseAction(lambda t: durations[t[0].lower()])
    for d in durations.keys()
]).set_parser_name("duration")("params")

duration = (realNum | intNum)("params") + _standard_time_intervals

interval = (
    INTERVAL + ("'" + delimitedList(duration) + "'" | duration)
).addParseAction(to_interval_call)

timestamp = (
    time_functions("op")
    + (
        sqlString("params")
        | MatchFirst([
            Keyword(t, caseless=True).addParseAction(lambda t: t.lower()) for t in times
        ])("params")
    )
).addParseAction(to_json_call)

extract = (
    Keyword("extract", caseless=True)("op")
    + LB
    + (_standard_time_intervals | expr("params"))
    + FROM
    + expr("params")
    + RB
).addParseAction(to_json_call)

namedColumn = Group(
    Group(expr)("value") + Optional(Optional(AS) + Group(ident))("name")
)

distinct = (
    DISTINCT("op") + delimitedList(namedColumn)("params")
).addParseAction(to_json_call)

ordered_sql = Forward()

call_function = (
    ident("op")
    + LB
    + Optional(Group(ordered_sql) | delimitedList(expr))("params")
    + Optional(
        Keyword("ignore", caseless=True) + Keyword("nulls", caseless=True)
    )("ignore_nulls")
    + RB
).addParseAction(to_json_call)

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
    | (LB + Group(ordered_sql) + RB)
    | (LB + Group(delimitedList(expr)).addParseAction(to_tuple_call) + RB)
    | sqlString.set_parser_name("string")
    | call_function
    | known_types
    | realNum.set_parser_name("float")
    | intNum.set_parser_name("int")
    | ident
)

expr << (
    (
        infixNotation(
            compound,
            [
                (
                    o,
                    1 if o in unary_ops else (3 if isinstance(o, tuple) else 2),
                    RIGHT_ASSOC if o in unary_ops else LEFT_ASSOC,
                    to_json_operator,
                )
                for o in KNOWN_OPS
            ],
        ).set_parser_name("expression")
    )("value")
    + Optional(window)
).addParseAction(to_expression_call)


alias = (
    (Group(ident) + Optional(LB + delimitedList(ident("col")) + RB))("name")
    .set_parser_name("alias")
    .addParseAction(to_alias)
)


selectColumn = (
    Group(
        Group(expr).set_parser_name("expression1")("value")
        + Optional(Optional(AS) + alias)
        | Literal("*")("value")
    )
    .set_parser_name("column")
    .addParseAction(to_select_call)
)


table_source = (
    ((LB + ordered_sql + RB) | call_function)("value").set_parser_name("table source")
    + Optional(Optional(AS) + alias)
    | (ident("value").set_parser_name("table name") + Optional(AS) + alias)
    | ident.set_parser_name("table name")
)

join = (
    (
        CROSS_JOIN
        | FULL_JOIN
        | FULL_OUTER_JOIN
        | INNER_JOIN
        | JOIN
        | LEFT_JOIN
        | LEFT_OUTER_JOIN
        | RIGHT_JOIN
        | RIGHT_OUTER_JOIN
    )("op")
    + Group(table_source)("join")
    + Optional((ON + expr("on")) | (USING + expr("using")))
).addParseAction(to_join_call)

unordered_sql = Group(
    SELECT
    + Optional(
        TOP
        + expr("value")
        + Optional(Keyword("percent", caseless=True))("percent")
        + Optional(WITH + Keyword("ties", caseless=True))("ties")
    )("top").addParseAction(to_top_clause)
    + delimitedList(selectColumn)("select")
    + Optional(
        (FROM + delimitedList(Group(table_source)) + ZeroOrMore(join))("from")
        + Optional(WHERE + expr("where"))
        + Optional(GROUP_BY + delimitedList(Group(namedColumn))("groupby"))
        + Optional(HAVING + expr("having"))
    )
).set_parser_name("unordered sql")

ordered_sql << (
    (unordered_sql + ZeroOrMore((UNION_ALL | UNION) + unordered_sql))("union")
    + Optional(ORDER_BY + delimitedList(Group(sortColumn))("orderby"))
    + Optional(LIMIT + expr("limit"))
    + Optional(OFFSET + expr("offset"))
).set_parser_name("ordered sql").addParseAction(to_union_call)

statement = Forward()
statement << (
    Optional(
        WITH + delimitedList(Group(ident("name") + AS + LB + statement("value") + RB))
    )("with")
    + Group(ordered_sql)("query")
).addParseAction(to_statement)

def to_create_table_call(instring, tokensStart, retTokens):
    t = retTokens.asDict()

    if t:
        return {"create table" : t}

createStmt = Forward()

column_name = ident.setDebugActions(*debug)

column_definition = Forward()

column_size = Group(
                Literal('(').suppress().setDebugActions(*debug) +
                delimitedList(
                    intNum.setName("size").setDebugActions(*debug)
                ) +
                Literal(')').suppress().setDebugActions(*debug)
              )

column_type = Forward()

BigQuery_STRUCT = (
    Keyword("struct", caseless=True)("type_name") + 
    Literal("<").suppress() +
    delimitedList(column_definition)("type_parameter") +
    Literal(">").suppress()
)

BigQuery_ARREY = (
    Keyword("array", caseless=True)("type_name") + 
    Literal("<").suppress() +
    delimitedList(column_type)("type_parameter") +
    Literal(">").suppress()
)

column_type << (
        BigQuery_STRUCT |
        BigQuery_ARREY |
        ident("type_name").setDebugActions(*debug) +
        Optional(column_size)("type_parameter")
    ).addParseAction(
        lambda s,l,t: { t.type_name: t.type_parameter } if t.type_parameter else t.type_name
    )

column_def_references = Group( 
    Keyword("references", caseless=True).suppress() + 
    Group( 
        ident("table") + 
        Literal("(").suppress() +
        delimitedList( ident )("columns") +
        Literal(")").suppress()
    )("references")
)

column_def_check = Group(
    Keyword("check", caseless=True).suppress() +
    Group( 
        Literal("(").suppress() +
        delimitedList( expr ) +
        Literal(")").suppress()
    )("check")
)

column_def_default = Group(
    Keyword("default", caseless=True).suppress() +
    Group( 
        ident | expr
    )("default")
)

column_options = ZeroOrMore( 
    Keyword("not null", caseless=True) 
    | Keyword("null", caseless=True) 
    | Keyword("unique", caseless=True) 
    | Keyword("primary key", caseless=True) 
    | column_def_references
    | column_def_check
    | column_def_default
)

column_definition << Group(
        column_name("name").addParseAction( lambda s, l, t: t.name.lower() ) +
        column_type("type") +
        Optional(column_options("option"))
    ).addParseAction(
        lambda s,l,t: t[0].asDict()
    )


createStmt << Group(
    CREATE_TABLE.suppress().setDebugActions(*debug) +
    (
        ident("name").setDebugActions(*debug) +
        Optional( 
            Literal("(").setDebugActions(*debug).suppress() +
            delimitedList(column_definition) +
            Literal(")").setDebugActions(*debug).suppress()
        )("columns") + 
        Optional( 
            AS.suppress() + 
            infixNotation( statement, [] )
        )("select_statement")
    ).addParseAction(to_create_table_call)
)

SQLParser = statement | createStmt
engine.release()
