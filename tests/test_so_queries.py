# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import os
from unittest import TestCase, skipIf

from mo_files import File
from mo_logs import Log

from mo_sql_parsing import parse
from mo_streams import content

IS_TRAVIS = bool(os.environ.get("TRAVIS"))


class TestSoQueries(TestCase):
    @skipIf(not IS_TRAVIS, "slow")
    def test_so_queries(self):
        def careful_parse(sql):
            if not sql:
                return
            try:
                parse(sql)
            except Exception as cause:
                Log.warning("failed", cause=cause)

        results = (
            File("tests/so_queries/so_queries.tar.zst")
            .content()
            .content()
            .exists()
            .utf8()
            .to_str()
            .map(careful_parse)
            .to_list()
        )
