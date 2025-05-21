# Copyright 2016-2018 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from random import Random
from secrets import randbelow, token_bytes
from sys import maxsize as max_int

DIGITS = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
BASE = 62
PREFIX_LENGTH = 12
SEQ_LENGTH = 10
MAX_SEQ = BASE**10
MIN_INC = 33
MAX_INC = 333
INC = MAX_INC - MIN_INC
TOTAL_LENGTH = PREFIX_LENGTH + SEQ_LENGTH


class NUID:
    """NUID created is a utility to create a new id.

    NUID is an implementation of the approach for fast generation
    of unique identifiers used for inboxes in NATS.
    """

    def __init__(self) -> None:
        self._prand = Random(randbelow(max_int))  # nosec B311  # noqa: S311
        self._seq = self._prand.randint(0, MAX_SEQ)
        self._inc = MIN_INC + self._prand.randint(BASE + 1, INC)
        self._prefix = bytearray()
        self.randomize_prefix()

    def next(self) -> bytearray:
        """Next returns the next unique identifier."""
        self._seq += self._inc
        if self._seq >= MAX_SEQ:
            self.randomize_prefix()
            self.reset_sequential()

        l_seq = self._seq
        prefix = self._prefix[:]
        suffix = bytearray(SEQ_LENGTH)
        for i in reversed(range(SEQ_LENGTH)):
            suffix[i] = DIGITS[int(l_seq) % BASE]
            l_seq //= BASE

        prefix.extend(suffix)
        return prefix

    def randomize_prefix(self) -> None:
        random_bytes = token_bytes(PREFIX_LENGTH)
        self._prefix = bytearray(DIGITS[c % BASE] for c in random_bytes)

    def reset_sequential(self) -> None:
        self._seq = self._prand.randint(0, MAX_SEQ)
        self._inc = MIN_INC + self._prand.randint(0, INC)
