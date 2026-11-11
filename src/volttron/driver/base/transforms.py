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
"""Point transforms: the ``Transform`` column of a registry, applied by :class:`DriverAgent` around the interface.

A point may declare a transform between the value its protocol carries (raw) and the value the platform publishes and
accepts (engineering): one function call (``scale(0.001)``, the modbus_tk form) or a sequence wrapped in
``transform(...)`` (``transform(scale(0.1), add(-40))``). Reads apply the chain forward, writes apply its inverse, and
the interface never sees the engineering value. The functions are those of ``icm.transforms`` (the ``icm-lib-transforms``
distribution), so a driver point and a resource transform share one implementation and one grammar.

:class:`TransformTable` holds the compiled transforms of one remote's points and does the work the agent needs:

- **configure time**: compile each point's text once (the library caches by text), resolve the other points a chain
  reads (``scale_reg``, ``sunssf`` ...) to full topics within the same device, and reject what cannot work: an unknown
  function or bad argument, a reference to a point the device does not have, a reference cycle, a register-only
  transform (``sum_of`` ...) on a point that is not ``STATIC``. A writable point whose chain has no inverse and a
  reference to a ``NEVER_POLL`` point are warned about and kept.
- **reads**: apply the forward transforms to a read's raw results in dependency order, so a scale factor read in the
  same request is used by the points that name it; look a factor up in the current read first and in the point's
  ``last_value`` second; when the factor has no value anywhere, or a function produced nothing, the point is left out
  of the results with an error entry, logged once until a value appears again.
- **writes**: the inverse of a point's chain, or a :class:`TransformValueError` when the chain cannot be inverted.
- **derived points**: a register-only transform defines a point from other points of the device. Such a point is
  ``virtual`` (:class:`DataSource`): never handed to the interface; the table computes it from the values of the points
  it references, reports which derived points a read's results make computable, and, for ``writable_bit``, names the
  real point a write to it is applied to.
"""
from __future__ import annotations

import logging

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from .config import DataSource, PointConfig
from .interfaces import BaseRegister, DriverInterfaceError

try:
    from icm.transforms import MISSING, ValueTransform, compile_value_transform
except ImportError:                                  # the base driver imports without the library; see configure()
    MISSING = object()
    ValueTransform = Any                             # type: ignore[assignment,misc]
    compile_value_transform = None

_log = logging.getLogger(__name__)

NOT_INSTALLED = ("the Transform column needs the icm-lib-transforms package, which is not installed"
                 " (pip install icm-lib-transforms)")
_TYPE_RANK = {bool: 0, int: 1, float: 2, str: 3}


class TransformConfigError(ValueError):
    """A point's transform cannot be configured: the point is left without one."""


class TransformValueError(DriverInterfaceError):
    """A transform produced no value (a missing reference, an undefined result) or cannot be inverted for a write."""


class VirtualRegister(BaseRegister):
    """The framework's own register for a virtual point, which no interface reads or writes."""

    def __init__(self, point_name: str, units: str, description: str = '', writable: bool = False):
        super().__init__('byte', not writable, point_name, units, description=description)
        self.python_type = float            # replaced by the computed type, or by the type of the first value set


DerivedRegister = VirtualRegister           # earlier name


@dataclass
class PointTransform:
    topic: str
    text: str
    transform: ValueTransform
    references: dict[str, str] = field(default_factory=dict)    #: referenced point name -> its full topic
    derived: bool = False                                        #: a register-only chain: defines the point's value
    write_target: str | None = None                              #: topic a write to this point is applied to, if any
    read_before_write: bool = False                              #: read that topic afresh before inverting
    held: bool = False                                           #: the hold-out message was logged and not yet cleared

    @property
    def invertible(self) -> bool:
        return self.transform.invertible


class _Failure:
    """Why a point got no value from its transform, carried through the results without raising."""
    __slots__ = ('message',)

    def __init__(self, message: str):
        self.message = message


Lookup = Callable[[str], Any]


class TransformTable:
    """The compiled transforms of one remote's points, keyed by full topic."""
    # TODO: One column serves reads and writes: a write inverts the read chain or is refused. A separate
    #  ``Write Transform`` column (its own chain for writes, covering non-invertible read chains) was considered in
    #  DRIVER_TRANSFORMS_PLAN.md section 4 and deferred until a device needs it.

    def __init__(self, label: str = ''):
        self.label = label
        self._points: dict[str, PointTransform] = {}
        self._dependents: dict[str, set[str]] = {}       #: referenced topic -> topics whose transforms read it
        self.derived: dict[str, PointTransform] = {}     #: the register-only (derived) entries, by topic

    def __contains__(self, topic: str) -> bool:
        return topic in self._points

    def __len__(self) -> int:
        return len(self._points)

    def get(self, topic: str) -> PointTransform | None:
        return self._points.get(topic)

    @property
    def referenced_topics(self) -> set[str]:
        """Every topic some point's transform reads."""
        return {topic for topic, dependents in self._dependents.items() if dependents}

    # ---- configure time ----------------------------------------------------------------------------------------------
    def configure(self, point_config: PointConfig, device_topic: str, device_points: Mapping[str, PointConfig]
                  ) -> PointTransform | None:
        """Compile the point's transform text. None for an empty column (any earlier transform of the topic is dropped).

        ``device_points`` maps every point name of the device to its configuration (the point itself included); it is
        what a referenced name is checked against. Raises :class:`TransformConfigError` when the transform cannot be
        configured; the caller logs it and leaves the point without a transform.
        """
        topic = '/'.join([device_topic, point_config.volttron_point_name])
        text = (point_config.transform or '').strip()
        self.remove(topic)
        if not text:
            return None
        if compile_value_transform is None:
            raise TransformConfigError(f'{NOT_INSTALLED}; {text!r} cannot be applied.')
        try:
            transform = compile_value_transform(text)
        except Exception as e:                           # the library raises ValueError and its UnknownFunction subclass
            raise TransformConfigError(str(e)) from None
        if transform.identity:
            return None
        derived = getattr(transform, 'defines_value', None)
        if derived is None:                              # an older library: fall back to the _of naming convention
            derived = bool(transform.functions) and transform.functions[0].endswith('_of')
        name = point_config.volttron_point_name
        if derived and point_config.data_source is not DataSource.VIRTUAL:
            raise TransformConfigError(f'{text!r} defines the point from other registers, so {name} must have'
                                       f' Data Source virtual, not {point_config.data_source.value}.')
        references: dict[str, str] = {}
        resolved_names: dict[str, str] = {}
        for ref in sorted(transform.references):
            resolved = self._resolve_reference(ref, device_points)
            if resolved == name:
                raise TransformConfigError(f'{text!r}: {name} cannot reference itself.')
            resolved_names[ref] = resolved
            references[ref] = '/'.join([device_topic, resolved])
        entry = PointTransform(topic, text, transform, references, derived)
        if target := getattr(transform, 'write_target', None):
            entry.write_target = references.get(target) or '/'.join([device_topic, self._resolve_reference(target, device_points)])
            entry.read_before_write = bool(getattr(transform, 'read_before_write', False))
        if cycle := self._find_cycle(entry):
            raise TransformConfigError(f'{text!r}: the references form a cycle: {" -> ".join(cycle)}.')
        if point_config.writable and not derived and not transform.invertible:
            _log.warning(f'{self.label}: {topic} is writable but its transform {text!r} cannot be inverted'
                         f' ({transform.non_invertible[0]!r} has no inverse): writes to it will be refused.')
        for ref, resolved in resolved_names.items():
            source = getattr(device_points[resolved], 'data_source', None)
            if source is DataSource.NEVER_POLL:
                _log.warning(f'{self.label}: {topic} reads {ref}, whose Data Source is never_poll. The point is held'
                             f' out of publication until {ref} receives a value by a push, a write or a reconfiguration.')
        self._points[topic] = entry
        for ref_topic in references.values():
            self._dependents.setdefault(ref_topic, set()).add(topic)
        if derived:
            self.derived[topic] = entry
        return entry

    @staticmethod
    def _resolve_reference(ref: str, device_points: Mapping[str, PointConfig]) -> str:
        """The Volttron Point Name a referenced name denotes.

        The name itself when it is a point of the device. Otherwise the one row whose ``Register Name`` (a modbus_tk
        register map names its own registers, which the device's registry may publish under other names) or
        ``Reference Point Name`` is that name. Rows without those fields are never candidates.
        """
        if ref in device_points:
            return ref
        matches = sorted(name for name, config in device_points.items()
                         if ref in (getattr(config, 'register_name', None), getattr(config, 'reference_point_name', None)))
        if len(matches) == 1:
            return matches[0]
        if matches:
            raise TransformConfigError(f'reference {ref!r} is ambiguous: it is the register name of points'
                                       f' {", ".join(matches)}.')
        raise TransformConfigError(f'reference {ref!r} is not a point of this device (by Volttron Point Name,'
                                   f' Register Name or Reference Point Name).')

    def remove(self, topic: str):
        entry = self._points.pop(topic, None)
        self.derived.pop(topic, None)
        if entry is not None:
            for ref_topic in entry.references.values():
                dependents = self._dependents.get(ref_topic)
                if dependents is not None:
                    dependents.discard(topic)
                    if not dependents:
                        del self._dependents[ref_topic]

    def _find_cycle(self, entry: PointTransform) -> list[str] | None:
        """The chain of topics leading from entry back to itself through configured references, if any."""
        stack: list[tuple[str, list[str]]] = [(ref, [entry.topic, ref]) for ref in entry.references.values()]
        seen: set[str] = set()
        while stack:
            topic, path = stack.pop()
            if topic == entry.topic:
                return path
            if topic in seen:
                continue
            seen.add(topic)
            other = self._points.get(topic)
            if other is not None:
                stack.extend((ref, path + [ref]) for ref in other.references.values())
        return None

    def result_type(self, topic: str, get_register: Callable[[str], BaseRegister]) -> type | None:
        """The Python type the point's transform yields, or None when it leaves the register's type alone.

        A derived chain that declares no type (``sum_of`` over integers) takes the widest type among the registers
        it references, which are all known once the device is configured.
        """
        entry = self._points.get(topic)
        if entry is None:
            return None
        if entry.transform.result_type is not None:
            return entry.transform.result_type
        if not entry.derived:
            return None
        types = []
        for ref_topic in entry.references.values():
            try:
                types.append(get_register(ref_topic).python_type)
            except DriverInterfaceError:
                continue
        return max(types, key=lambda t: _TYPE_RANK.get(t, -1), default=None) if types else None

    # ---- run time ---------------------------------------------------------------------------------------------------
    def apply(self, results: dict[str, Any], errors: dict[str, Any], last_value: Lookup) -> None:
        """Replace the raw values in ``results`` with their engineering values, in place.

        Points whose transforms read other points are evaluated after those points, so a factor read in the same
        request is used before any ``last_value``. A point whose transform fails is removed from ``results`` and gets
        an entry in ``errors``.
        """
        pending = {topic for topic in results if (entry := self._points.get(topic)) is not None and not entry.derived}
        if not pending:
            return
        done: dict[str, Any] = {}

        def resolve(topic: str, active: set[str]):
            if topic in done:
                return done[topic]
            if topic in pending and topic not in active:
                active.add(topic)
                value = self._forward(self._points[topic], results[topic], lambda t: resolve(t, active))
                active.discard(topic)
                done[topic] = value
                return value
            return results[topic] if topic in results and topic not in pending else last_value(topic)

        for topic in pending:
            resolve(topic, set())
        for topic in pending:
            value = done[topic]
            if isinstance(value, _Failure):
                results.pop(topic, None)
                errors[topic] = value.message
            else:
                results[topic] = value

    def forward(self, topic: str, raw: Any, lookup: Lookup) -> Any:
        """The engineering value of ``raw`` for one point; raw itself for a point without a transform.

        Raises :class:`TransformValueError` when the transform produces no value.
        """
        entry = self._points.get(topic)
        if entry is None or entry.derived:
            return raw
        value = self._forward(entry, raw, lookup)
        if isinstance(value, _Failure):
            raise TransformValueError(value.message)
        return value

    def forward_or_none(self, topic: str, raw: Any, lookup: Lookup) -> Any:
        """As :meth:`forward`, but None instead of an exception (for the value an interface reports after a write)."""
        try:
            return self.forward(topic, raw, lookup)
        except TransformValueError as e:
            _log.debug(f'{self.label}: no engineering value for {topic} after a write: {e}')
            return None

    def to_raw(self, topic: str, value: Any, lookup: Lookup) -> Any:
        """The raw value to write for engineering ``value``; value itself for a point without a transform.

        Raises :class:`TransformValueError` for a derived point, a chain without an inverse, or an inverse that
        produces no value (a factor without a value).
        """
        entry = self._points.get(topic)
        if entry is None:
            return value
        if entry.write_target is not None:
            raise TransformValueError(f'{topic} is written through {entry.write_target}; use write_through().')
        if entry.derived:
            raise TransformValueError(f'{topic} is a derived point: its value is defined by {entry.text!r}'
                                      ' and cannot be set.')
        if not entry.invertible:
            raise TransformValueError(f'{topic} cannot be written: its transform {entry.text!r} cannot be inverted'
                                      f' ({entry.transform.non_invertible[0]!r} has no inverse).')
        values = self._values(entry, lookup)
        try:
            raw = entry.transform.inverse(value, values)
        except Exception as e:
            raise TransformValueError(f'{topic}: inverting {entry.text!r} failed: {e}') from e
        if raw is MISSING:
            raise TransformValueError(f'{topic}: {entry.text!r} cannot be inverted now: {self._why_missing(entry, values)}')
        return raw

    def write_through(self, topic: str, value: Any, lookup: Lookup) -> tuple[str, Any]:
        """For a point written through another (``writable_bit``): the target topic and the engineering value to
        write to it, from ``value`` and the target's current value given by ``lookup``."""
        entry = self._points[topic]
        values = self._values(entry, lookup)
        try:
            target_value = entry.transform.inverse(value, values)
        except Exception as e:
            raise TransformValueError(f'{topic}: {entry.text!r} could not produce a value for {entry.write_target}: {e}') from e
        if target_value is MISSING:
            raise TransformValueError(f'{topic}: {entry.text!r} cannot be written now: {self._why_missing(entry, values)}')
        return entry.write_target, target_value

    def compute(self, topics: Iterable[str], lookup: Lookup) -> tuple[dict[str, Any], dict[str, str]]:
        """The values of the derived points named, from the values ``lookup`` gives their references."""
        results, errors = {}, {}
        for topic in topics:
            entry = self._points.get(topic)
            if entry is None or not entry.derived:
                errors[topic] = f'{topic} is not a derived point.'
                continue
            value = self._forward(entry, None, lookup)
            if isinstance(value, _Failure):
                errors[topic] = value.message
            else:
                results[topic] = value
        return results, errors

    def dependents(self, topics: Iterable[str]) -> set[str]:
        """The derived points whose transforms read any of ``topics``."""
        found: set[str] = set()
        for topic in topics:
            for dependent in self._dependents.get(topic, ()):
                if dependent in self.derived:
                    found.add(dependent)
        return found

    def compute_dependents(self, results: Mapping[str, Any], last_value: Lookup) -> tuple[dict[str, Any], dict[str, str]]:
        """The derived points a read's engineering ``results`` make computable, from those results and last values."""
        dependents = self.dependents(results)
        if not dependents:
            return {}, {}
        return self.compute(sorted(dependents), lambda t: results[t] if t in results else last_value(t))

    # ---- helpers ----------------------------------------------------------------------------------------------------
    def _values(self, entry: PointTransform, lookup: Lookup) -> dict[str, Any]:
        return {name: lookup(topic) for name, topic in entry.references.items()}

    def _forward(self, entry: PointTransform, raw: Any, lookup: Lookup):
        values = self._values(entry, lookup) if entry.references else None
        try:
            value = entry.transform.forward(raw, values)
        except Exception as e:
            message = f'{entry.text!r} failed: {e}'
            _log.warning(f'{self.label}: {entry.topic}: {message}')
            return _Failure(message)
        if value is MISSING:
            message = f'{entry.text!r} produced no value: {self._why_missing(entry, values or {}, raw)}'
            if not entry.held:
                entry.held = True
                _log.warning(f'{self.label}: {entry.topic}: {message} No value is published for the point until'
                             ' one can be computed.')
            return _Failure(message)
        if entry.held:
            entry.held = False
            _log.info(f'{self.label}: {entry.topic}: {entry.text!r} produces a value again.')
        return value

    @staticmethod
    def _why_missing(entry: PointTransform, values: Mapping[str, Any], raw: Any = 0) -> str:
        absent = sorted(name for name in entry.references if values.get(name) is None)
        if absent:
            return f'referenced point(s) {", ".join(absent)} have no value yet'
        if raw is None:
            return 'the point itself has no value'
        return ('a function in the chain produced no value (an invalid or not-implemented raw value, a division by zero,'
                ' a lookup that found nothing)')
