# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Beto Dealmeida (beto@dealmeida.net)
#

from __future__ import absolute_import, division, unicode_literals

from unittest import skip

from mo_files import File
from mo_logs import Log
from mo_testing.fuzzytestcase import FuzzyTestCase

from mo_sql_parsing import parse, format


class TestSimple(FuzzyTestCase):
    @skip("too long")
    def test_files(self):
        for file in File("tests/sql").leaves:
            for i, line in enumerate(file.read_lines()):
                if line.startswith("#"):
                    print(line)
                    continue
                query = line.split("\t")[0]
                query = query.rstrip(";")
                try:
                    parse_result = parse(query)
                    format_result = format(parse_result)
                    self.assertEqual(
                        (
                            format_result
                            .lower()
                            .replace(" ", "")
                        ),
                        query.lower().replace(" ", ""),
                    )
                except Exception as cause:
                    Log.error(
                        """file {{file}}, line {{line}}: unable to parse-and-format {{sql|quote}}""",
                        file=file.name,
                        line=i + 1,
                        sql=query,
                        cause=cause,
                    )
