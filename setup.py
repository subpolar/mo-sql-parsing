# encoding: utf-8
# THIS FILE IS AUTOGENERATED!
from __future__ import unicode_literals
from setuptools import setup
setup(
    author='Kyle Lahnakoski',
    author_email='kyle@lahnakoski.com',
    classifiers=["Development Status :: 3 - Alpha","Topic :: Software Development :: Libraries","Topic :: Software Development :: Libraries :: Python Modules","Programming Language :: SQL","Programming Language :: Python :: 3.7","License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)"],
    description='Extract Parse Tree from SQL',
    include_package_data=True,
    install_requires=["mo-dots==3.141.20326","mo-future==3.136.20306","mo-logs==3.143.20326"],
    license='MPL 2.0',
    long_description='# Moz SQL Parser\n\nLet\'s make a SQL parser so we can provide a familiar interface to non-sql datastores!\n\n\n|Branch      |Status   |\n|------------|---------|\n|master      | [![Build Status](https://travis-ci.org/klahnakoski/moz-sql-parser.svg?branch=master)](https://travis-ci.org/mozilla/moz-sql-parser) |\n|dev         | [![Build Status](https://travis-ci.org/klahnakoski/moz-sql-parser.svg?branch=dev)](https://travis-ci.org/mozilla/moz-sql-parser)    |\n\n\n## Problem Statement\n\nSQL is a familiar language used to access databases. Although, each database vendor has its quirky implementation, the average developer does not know enough SQL to be concerned with those quirks. This familiar core SQL (lowest common denominator, if you will) is useful enough to explore data in primitive ways. It is hoped that, once programmers have reviewed a datastore with basic SQL queries, and they see the value of that data, they will be motivated to use the datastore\'s native query format.\n\n## Objectives\n\nThe primary objective of this library is to convert some subset of [SQL-92](https://en.wikipedia.org/wiki/SQL-92) queries to JSON-izable parse trees. A big enough subset to provide superficial data access via SQL, but not so much as we must deal with the document-relational impedance mismatch.\n\n## Non-Objectives \n\n* No plans to provide update statements, like `update` or `insert`\n* No plans to expand the language to all of SQL:2011\n* No plans to provide data access tools \n\n\n## Project Status\n\nThere are [over 400 tests](https://github.com/mozilla/moz-sql-parser/tree/dev/tests). This parser is good enough for basic usage, including inner queries.\n\nYou can see the parser in action at [https://sql.telemetry.mozilla.org/](https://sql.telemetry.mozilla.org/) while using the ActiveData datasource\n\n## Install\n\n    pip install moz-sql-parser\n\n## Parsing SQL\n\n    >>> from moz_sql_parser import parse\n    >>> import json\n    >>> json.dumps(parse("select count(1) from jobs"))\n    \'{"select": {"value": {"count": 1}}, "from": "jobs"}\'\n    \nEach SQL query is parsed to an object: Each clause is assigned to an object property of the same name. \n\n    >>> json.dumps(parse("select a as hello, b as world from jobs"))\n    \'{"select": [{"value": "a", "name": "hello"}, {"value": "b", "name": "world"}], "from": "jobs"}\'\n\nThe `SELECT` clause is an array of objects containing `name` and `value` properties. \n\n### Recursion Limit \n\nPython\'s default recursion limit (1000) is not hit when parsing the test suite, but this may not be the case for large SQL. You can increase the recursion limit before you `parse`:\n\n    >>> from moz_sql_parser import parse\n    >>> sys.setrecursionlimit(3000)\n    >>> parse(complicated_sql)\n\n\n## Generating SQL\n\nYou may also generate SQL from the a given JSON document. This is done by the formatter, which is still incomplete (Jan2020).\n\n    >>> from moz_sql_parser import format\n    >>> format({"from":"test", "select":["a.b", "c"]})\n    \'SELECT a.b, c FROM test\'\n\n## Contributing\n\nIn the event that the parser is not working for you, you can help make this better but simply pasting your sql (or JSON) into a new issue. Extra points if you describe the problem. Even more points if you submit a PR with a test.  If you also submit a fix, then you also have my gratitude. \n\n\n## Run Tests\n\nSee [the tests directory](https://github.com/mozilla/moz-sql-parser/tree/dev/tests) for instructions running tests, or writing new ones.\n\n## More about implementation\n\nSQL queries are translated to JSON objects: Each clause is assigned to an object property of the same name.\n\n    \n    # SELECT * FROM dual WHERE a>b ORDER BY a+b\n    {\n        "select": "*", \n        "from": "dual", \n        "where": {"gt": ["a", "b"]}, \n        "orderby": {"value": {"add": ["a", "b"]}}\n    }\n        \nExpressions are also objects, but with only one property: The name of the operation, and the value holding (an array of) parameters for that operation. \n\n    {op: parameters}\n\nand you can see this pattern in the previous example:\n\n    {"gt": ["a","b"]}\n\n',
    long_description_content_type='text/markdown',
    name='moz-sql-parser',
    packages=["moz_sql_parser","mo_parsing"],
    url='https://github.com/klahnakoski/moz-sql-parser',
    version='3.146.20326',
    zip_safe=True
)