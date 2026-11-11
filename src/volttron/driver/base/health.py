# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2025 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===
# }}}
"""Health of one remote, as every driver can report it.

:class:`RemoteHealth` is what :class:`DriverAgent` knows from its own traffic: whether polls return values, how many
have failed in a row, when the last one succeeded, which points failed last and why, and how writes fare. An
interface adds what its protocol knows through an optional ``health()`` method (a proxy's connection counters, a
device's identification), and the record carries free-form ``events`` (a proxy process lost, a device identified)
with the time they were noted.

Status is ``UNKNOWN`` until the first poll, ``GOOD`` while polls return values, and ``BAD`` once a poll returns no
value at all (or raises). A poll that returns some values with errors for others is ``GOOD`` with the errors listed
per point, since the device is answering.
"""
from __future__ import annotations

import time

from collections import deque
from typing import Any, Callable

STATUS_UNKNOWN, STATUS_GOOD, STATUS_BAD = 'UNKNOWN', 'GOOD', 'BAD'


def _iso(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(timestamp)) + f'{timestamp % 1:.3f}'[1:] + 'Z'


class RemoteHealth:
    """What the driver framework observes about one remote. ``on_change`` is called with the record when the status
    changes between GOOD and BAD (and on the first poll)."""

    MAX_EVENTS = 20

    def __init__(self, unique_id: Any, on_change: Callable[['RemoteHealth'], None] | None = None):
        self.unique_id = unique_id
        self.on_change = on_change
        self.status = STATUS_UNKNOWN
        self.polls = 0
        self.poll_failures = 0                 # polls that returned nothing (or raised)
        self.consecutive_failures = 0
        self.last_success: float | None = None
        self.last_failure: float | None = None
        self.last_error: str | None = None
        self.last_poll_duration: float | None = None
        self.point_errors: dict[str, str] = {}  # the points that failed in the latest poll, with the message
        self.writes = 0
        self.write_failures = 0
        self.last_write_error: str | None = None
        self.events: deque[tuple[float, str]] = deque(maxlen=self.MAX_EVENTS)

    # ---- recording -----------------------------------------------------------------------------------------------------
    def record_poll(self, results: dict, errors: dict, duration: float | None = None, exception: BaseException | None = None):
        self.polls += 1
        self.last_poll_duration = duration
        now = time.time()
        if exception is None and results:
            self.consecutive_failures = 0
            self.last_success = now
            self.point_errors = {str(topic): str(message) for topic, message in errors.items()}
            self._set_status(STATUS_GOOD)
            return
        self.poll_failures += 1
        self.consecutive_failures += 1
        self.last_failure = now
        if exception is not None:
            self.last_error = f'{type(exception).__name__}: {exception}'
            self.point_errors = {}
        else:
            self.point_errors = {str(topic): str(message) for topic, message in errors.items()}
            self.last_error = next(iter(self.point_errors.values()), 'No values returned.')
        self._set_status(STATUS_BAD)

    def record_write(self, errors: dict):
        self.writes += 1
        if errors:
            self.write_failures += 1
            self.last_write_error = str(next(iter(errors.values())))

    def note(self, event: str):
        """Keep a protocol or lifecycle event (a proxy lost, a device identified) with its time."""
        self.events.append((time.time(), event))

    def _set_status(self, status: str):
        changed = status != self.status
        self.status = status
        if changed and self.on_change is not None:
            self.on_change(self)

    # ---- reporting -----------------------------------------------------------------------------------------------------
    def as_dict(self, protocol: dict | None = None) -> dict[str, Any]:
        """The record as JSON-friendly data, with the interface's protocol health under ``protocol`` when given."""
        record = {
            'status': self.status,
            'polls': self.polls, 'poll_failures': self.poll_failures, 'consecutive_failures': self.consecutive_failures,
            'last_success': _iso(self.last_success), 'last_failure': _iso(self.last_failure), 'last_error': self.last_error,
            'last_poll_duration': self.last_poll_duration,
            'point_errors': dict(self.point_errors),
            'writes': self.writes, 'write_failures': self.write_failures, 'last_write_error': self.last_write_error,
            'events': [{'time': _iso(t), 'event': e} for t, e in self.events],
        }
        if protocol is not None:
            record['protocol'] = protocol
        return record
