# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2022 Battelle Memorial Institute
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

import gevent
import logging
import time
import traceback

from collections import defaultdict
from typing import Any, cast
from weakref import WeakSet

from importlib.metadata import distribution, PackageNotFoundError
try:
    distribution('volttron-core')
    from volttron.client.vip.agent import Agent
except PackageNotFoundError:
    from volttron.platform.vip.agent import Agent

from .interfaces import BaseInterface, BaseRegister, DriverInterfaceError
from .config import DataSource, PointConfig, RemoteConfig
from .health import RemoteHealth
from .transforms import TransformConfigError, TransformTable, TransformValueError, VirtualRegister
from .utils import publication_headers, publish_wrapper
# from platform_driver.poll_scheduler import PollSet  # TODO: This should not import from driver. Need to relocate PollSet.

_log = logging.getLogger(__name__)


class DriverAgent:
    def __init__(self, config: RemoteConfig, core, equipment_model, scalability_test, tz: str, unique_id: Any,
                 vip: Agent.Subsystems):
        self.core = core
        self.equipment_model = equipment_model  # TODO: This should probably move out of the agent and into the base or a library.
        self.scalability_test = scalability_test  # TODO: If this is used from here, it should probably be in the base driver.
        self.tz: str = tz  # TODO: This won't update here if it is updated in the agent. Can it's use be moved out of here?
        self.unique_id: Any = unique_id
        self.vip: Agent.Subsystems = vip
        
        # State Variables
        self.equipment = WeakSet()
        self.heart_beat_value = 0
        self.interface = None
        self.publishers = {}
        # The Transform column of this remote's points (volttron.driver.base.transforms): the interface works in raw
        # values, everything this class hands to or takes from the platform is in engineering values.
        self.transforms = TransformTable(str(unique_id))
        # Virtual points (DataSource.VIRTUAL): held here, never by the interface.
        self.virtual: dict[str, VirtualRegister] = {}
        # What this remote's traffic says about it (volttron.driver.base.health); the platform driver sets on_change
        # to be told when a remote starts or stops answering.
        self.health = RemoteHealth(unique_id)

        try:
            klass = BaseInterface.get_interface_subclass(config.driver_type)
            interface = klass(config, self)
            self.interface = cast(BaseInterface, interface)
        except ValueError as e:
            _log.error(f"Failed to setup device: {e}")
            raise e

        self.config: RemoteConfig = self.interface.config

    def add_registers(self, registry_config: list[PointConfig], base_topic: str):
        """
        Configure a set of registers on this remote.

        :param registry_config: A list of registry points represented as PointConfigs
        :param base_topic: The portion of the topic shared by all points in this registry.
        """
        registry_config = list(registry_config)
        device_points = {p.volttron_point_name: p for p in registry_config}
        failed: list[str] = []
        for register_config in registry_config:
            topic = '/'.join([base_topic, register_config.volttron_point_name])
            try:
                self.add_register(register_config, base_topic, device_points, update_metadata=False)
            except Exception as e:
                # One bad row never takes the device down: the row is dropped and the rest configure.
                _log.error(f'{self.unique_id}: point {topic} is not configured: {e}')
                failed.append(topic)
        self._drop_failed_points(failed)
        # TODO: Finalize method is only used by bacnet, currently, and that pauses 30s on each device if it can't connect.
        try:
            self.interface.finalize_setup(initial_setup=True)
        except BaseException as e:
            _log.warning(f'Exception occurred while finalizing setup of interface for {self.unique_id}: {e}.')
            self.health.note(f'setup failed: {e}')

        for point_name in self.get_register_names():
            self.update_metadata(point_name)
        return failed

    def _drop_failed_points(self, topics: list[str]):
        """Remove the equipment model's nodes for points whose registers could not be created, so they are neither
        scheduled nor reported as unconfigured on every poll."""
        remove = getattr(self.equipment_model, 'remove_segment', None)
        for topic in topics:
            self.remove_register(topic)
            if callable(remove) and self.equipment_model.get_node(topic) is not None:
                try:
                    remove(topic)
                except Exception as e:
                    _log.warning(f'{self.unique_id}: unable to remove the node of unconfigured point {topic}: {e}')

    def add_register(self, point_config: PointConfig, base_topic: str, device_points: dict[str, PointConfig] | None = None,
                     update_metadata: bool = True) -> BaseRegister:
        """Configure one point of the device at ``base_topic``: its transform, then its register.

        A virtual point (``Data Source`` virtual: set by the platform, or defined from other points by its transform)
        is not handed to the interface; the framework keeps a register of its own for it. A transform that cannot be
        configured is logged and the point is configured without one. ``device_points`` is the device's registry by
        point name, for checking references; when omitted it is taken from the equipment model.
        """
        topic = '/'.join([base_topic, point_config.volttron_point_name])
        if device_points is None:
            device_points = self._device_point_configs(base_topic)
            device_points[point_config.volttron_point_name] = point_config
        try:
            self.transforms.configure(point_config, base_topic, device_points)
        except TransformConfigError as e:
            _log.error(f'{self.unique_id}: {topic}: {e} The point is configured without a transform.')
        if point_config.data_source is DataSource.VIRTUAL:
            register = VirtualRegister(point_config.volttron_point_name, point_config.units, point_config.notes,
                                       writable=point_config.writable)
            self.virtual[topic] = register
            self.interface.point_map.pop(topic, None)       # it may have been a device point before this update
        else:
            self.virtual.pop(topic, None)
            register = self.interface.create_register(point_config)
            self.interface.insert_register(register, base_topic)
        if update_metadata:
            self.update_metadata(topic)
        return register

    def remove_register(self, topic: str):
        """Forget a point: its transform, its virtual register, and the interface's register."""
        self.transforms.remove(topic)
        self.virtual.pop(topic, None)
        self.interface.point_map.pop(topic, None)

    def get_register(self, topic: str) -> BaseRegister:
        """The register of a point of this remote: the framework's own for a virtual point, else the interface's."""
        virtual = self.virtual.get(topic)
        return virtual if virtual is not None else self.interface.get_register_by_name(topic)

    def get_register_names(self) -> list[str]:
        return list(self.interface.get_register_names()) + list(self.virtual)

    def _device_point_configs(self, device_topic: str) -> dict[str, PointConfig]:
        device_points = getattr(self.equipment_model, 'device_points', None)
        if not callable(device_points):
            return {}
        return {p.config.volttron_point_name: p.config for p in device_points(device_topic)}

    def update_metadata(self, point_name):
        register = self.get_register(point_name)
        point = self.equipment_model.get_node(point_name)
        # A transform may change the type (scale yields a float, equals a boolean); otherwise the register's type stands.
        python_type = self.transforms.result_type(point_name, self.get_register)
        if python_type is None:
            python_type = register.python_type
        elif point_name in self.virtual:
            register.python_type = python_type
        # TODO: Move the mapping logic hereto an abstract method in the Interface or Register classes?
        if python_type is bool or (register.register_type == 'bit' and point_name not in self.transforms):
            ts_type = 'boolean'
        elif python_type is int:
            ts_type = 'integer'
        elif python_type is float:
            ts_type = 'float'
        elif python_type is str:
            ts_type = 'string'
        else:
            ts_type = str(python_type)  # TODO: Should this just be "undefined" or something?
        point.meta_data = {
            'units': register.get_units(),
            'type': ts_type,
            'tz': self.tz
        }

    # ---- transforms: the interface works in raw values, the platform in engineering values ------------------------
    def _last_value(self, topic: str):
        node = self.equipment_model.get_node(topic)
        return getattr(node, 'last_value', None) if node is not None else None

    def _lookup(self, results: dict):
        """Values for a transform's references: the current read first, then the equipment model's last values."""
        return lambda topic: results[topic] if topic in results else self._last_value(topic)

    def to_raw(self, topic: str, value: Any) -> Any:
        """The raw value to write for an engineering ``value``; raises DriverInterfaceError when it cannot be inverted."""
        return self.transforms.to_raw(topic, value, self._lookup({}))

    @property
    def startup_read_topics(self) -> list[str]:
        """The polled points other points' transforms read: read once at setup so they have a last value before the
        first cyclic poll of anything that depends on them."""
        topics = []
        for topic in sorted(self.transforms.referenced_topics):
            source = getattr(self.equipment_model.get_node(topic), 'data_source', None)
            if getattr(source, 'polled', False):
                topics.append(topic)
        return topics

    def poll_data(self, poll_set): # PollSet):
        if self.scalability_test:  # TODO: Update scalability testing.
            self.scalability_test.poll_starting(self.unique_id)
        try:
            if not poll_set.points:
                return False
            started = time.monotonic()
            results, errors = self.get_multiple_points(poll_set.points.keys())
            # Derived points are computed with the reads of the points they reference and published as though polled.
            derived_results, derived_errors = self.transforms.compute_dependents(results, self._last_value)
            results.update(derived_results)
            errors.update(derived_errors)
            self.health.record_poll(results, errors, time.monotonic() - started)
            for failed_point, failure_message in errors.items():
                _log.warning(f'Failed to poll {failed_point}: {failure_message}')
            if results:
                for topic, value in results.items():
                    point = poll_set.points.get(topic) or self.equipment_model.get_node(topic)
                    if point and self.equipment_model.is_active(topic):
                        point.last_value = value
                self.publish_poll(results, poll_set)
            return True  # TODO: There could really be better logic in the method to measure success.
        except (Exception, gevent.Timeout) as e:
            self.health.record_poll({}, {}, None, exception=e)
            _log.error(f'Exception while polling {self.unique_id}: {e}')
            if self.config.debug:
                # TODO: Add an RPC to turn on debugging for individual remotes. Maybe for nodes as well?
                tb = traceback.format_exc()
                _log.error(tb)
            return False
        finally:
            if self.scalability_test:
                self.scalability_test.poll_ending(self.unique_id)

    def _publish_targets(self, topics):
        """Where points outside a poll set (derived points computed with it) are published: the same four groupings a
        PollSet keeps for its own points."""
        et = self.equipment_model
        single_depth, single_breadth, multi_depth, multi_breadth = set(), set(), defaultdict(set), defaultdict(set)
        for topic in topics:
            try:
                point_depth, point_breadth = et.get_point_topics(topic)
                device_depth, device_breadth = et.get_device_topics(topic)
                if et.is_published_single_depth(topic):
                    single_depth.add(point_depth)
                if et.is_published_single_breadth(topic):
                    single_breadth.add((point_depth, point_breadth))
                if et.is_published_multi_depth(topic):
                    multi_depth[device_depth].add(point_depth)
                if et.is_published_multi_breadth(topic):
                    multi_breadth[device_breadth].add(topic)
            except Exception as e:
                _log.warning(f'{self.unique_id}: unable to find where to publish {topic!r}: {e}')
        return single_depth, single_breadth, multi_depth, multi_breadth

    # noinspection DuplicatedCode
    def publish_poll(self, results, poll_set):
        headers = publication_headers()
        single_depth, single_breadth = poll_set.single_depth, poll_set.single_breadth
        multi_depth, multi_breadth = poll_set.multi_depth, poll_set.multi_breadth
        if extra := [topic for topic in results if topic not in poll_set.points]:
            x_single_depth, x_single_breadth, x_multi_depth, x_multi_breadth = self._publish_targets(extra)
            single_depth, single_breadth = single_depth | x_single_depth, single_breadth | x_single_breadth
            multi_depth = {k: multi_depth.get(k, set()) | x_multi_depth.get(k, set()) for k in multi_depth.keys() | x_multi_depth.keys()}
            multi_breadth = {k: multi_breadth.get(k, set()) | x_multi_breadth.get(k, set()) for k in multi_breadth.keys() | x_multi_breadth.keys()}
        for point_topic in single_depth:
            if point_topic in results:
                try:
                    publish_wrapper(self.vip, point_topic, headers=headers, message=[
                        results[point_topic], self.equipment_model.get_node(point_topic).meta_data
                    ])
                except Exception as e:
                    _log.warning(f'Failed to publish single_depth point_topic: {point_topic} -- {e}')
        for point_topic, publish_topic in single_breadth:
            if point_topic in results:
                try:
                    publish_wrapper(self.vip, publish_topic, headers=headers, message=[
                        results[point_topic], self.equipment_model.get_node(point_topic).meta_data
                    ])
                except Exception as e:
                    _log.warning(f'Failed to publish single_breadth publish_topic: {publish_topic}'
                                 f' for identifier: {point_topic} -- {e}')
        for device_topic, points in multi_depth.items():
            values = {point.rsplit('/', 1)[-1]: results[point] for point in points if point in results}
            if not values:
                _log.warning(f'No values were returned for any of the {len(points)} polled points of {device_topic}.'
                             f' Skipping the {device_topic}/multi publish.')
                continue
            try:
                publish_wrapper(self.vip, f'{device_topic}/multi', headers=headers, message=[
                    values,
                    {point.rsplit('/', 1)[-1]: self.equipment_model.get_node(point).meta_data for point in points}
                ])
            except Exception as e:
                _log.warning(f'Failed to publish multi_depth device_topic: {device_topic} -- {e}')
        for publish_topic, points in multi_breadth.items():
            values = {point.rsplit('/', 1)[-1]: results[point] for point in points if point in results}
            if not values:
                _log.warning(f'No values were returned for any of the {len(points)} polled points of {publish_topic}.'
                             f' Skipping the {publish_topic}/multi publish.')
                continue
            try:
                publish_wrapper(self.vip, f'{publish_topic}/multi', headers=headers, message=[
                    values,
                    {point.rsplit('/', 1)[-1]: self.equipment_model.get_node(point).meta_data for point in points}
                ])
            except Exception as e:
                _log.warning(f'Failed to publish multi_breadth publish_topic: {publish_topic} -- {e}')

    def heart_beat(self):
        if self.config.heart_beat_point is None:
            return
        heart_beat_value = int(not bool(self.heart_beat_value))
        self.set_point(self.config.heart_beat_point, heart_beat_value)
        self.heart_beat_value = heart_beat_value

    def get_point(self, topic, **kwargs):
        """The engineering value of one point. Read options (``kwargs``) select something other than the point's
        ordinary value (another BACnet property, say), so a read with options is returned as the interface gives it.
        A virtual point is computed from its references, or is its last value when nothing defines it."""
        if topic in self.transforms.derived:
            results, errors = self.transforms.compute([topic], self._lookup({}))
            if topic not in results:
                raise TransformValueError(errors[topic])
            return results[topic]
        if topic in self.virtual:
            return self._last_value(topic)
        raw = self.interface.get_point(topic, **kwargs)
        return self.transforms.forward(topic, raw, self._lookup({})) if not kwargs else raw

    def _set_virtual(self, topic, value, **kwargs):
        """Write a virtual point: through its write target when its transform names one (``writable_bit``), else by
        storing and publishing the value when the row is writable."""
        entry = self.transforms.get(topic)
        if entry is not None and entry.write_target is not None:
            lookup = self._lookup({})
            if entry.read_before_write:
                current = self.get_point(entry.write_target, **kwargs)
                lookup = self._lookup({entry.write_target: current})
            target, target_value = self.transforms.write_through(topic, value, lookup)
            self.set_point(target, target_value, **kwargs)
            computed, _ = self.transforms.compute([topic], self._lookup({target: target_value}))
            published = computed.get(topic, value)
        else:
            register = self.virtual[topic]
            if register.read_only:
                raise TransformValueError(f'{topic} is a virtual point that is not writable'
                                          + (f'; its value is defined by {entry.text!r}' if entry is not None else '') + '.')
            published = self._coerce_virtual(register, value)
        self.publish_push({topic: published})
        return published

    @staticmethod
    def _coerce_virtual(register: VirtualRegister, value):
        if register.python_type is float and not isinstance(value, (bool, int, float)):
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
        register.python_type = type(value)
        return value

    def set_point(self, topic, value, **kwargs):
        """Write an engineering value: the interface receives the raw value, and what it reports as set comes back as
        an engineering value (None when that cannot be computed)."""
        if topic in self.virtual:
            return self._set_virtual(topic, value, **kwargs)
        raw = self.to_raw(topic, value)
        try:
            result = self.interface.set_point(topic, raw, **kwargs)
        except Exception as e:
            self.health.record_write({topic: str(e)})
            raise
        self.health.record_write({})
        return self.transforms.forward_or_none(topic, result, self._lookup({}))

    def get_multiple_points(self, topics, **kwargs):
        """Engineering values of several points. Derived points are computed from the other values read and the last
        values; with read options nothing is transformed (see get_point) and derived points are not read."""
        topics = list(topics)
        derived = [t for t in topics if t in self.transforms.derived]
        held = [t for t in topics if t in self.virtual and t not in self.transforms.derived]
        plain = [t for t in topics if t not in self.virtual]
        results, errors = self.interface.get_multiple_points(plain, **kwargs) if plain else ({}, {})
        results, errors = dict(results), dict(errors)
        if kwargs:
            errors.update({t: f'{t} is a virtual point; it has no value for a read with options.' for t in derived + held})
            return results, errors
        self.transforms.apply(results, errors, self._last_value)
        if derived:
            derived_results, derived_errors = self.transforms.compute(derived, self._lookup(results))
            results.update(derived_results)
            errors.update(derived_errors)
        for topic in held:
            results[topic] = self._last_value(topic)
        return results, errors

    def set_multiple_points(self, topics_values, **kwargs):
        raw_items, errors, results = [], {}, {}
        for topic, value in topics_values:
            try:
                if topic in self.virtual:
                    results[topic] = self._set_virtual(topic, value, **kwargs)
                else:
                    raw_items.append((topic, self.to_raw(topic, value)))
            except DriverInterfaceError as e:
                errors[topic] = str(e)
        written, interface_errors = self.interface.set_multiple_points(raw_items, **kwargs) if raw_items else ({}, {})
        lookup = self._lookup({})
        results.update({topic: self.transforms.forward_or_none(topic, raw, lookup) for topic, raw in written.items()})
        errors.update(interface_errors)
        if raw_items:
            self.health.record_write(interface_errors)
            self._publish_static_writes({topic: value for topic, value in topics_values if topic in written}, results)
        return results, errors

    def _static_points(self, topics) -> list:
        nodes = []
        for topic in topics:
            node = self.equipment_model.get_node(topic)
            if node is not None and getattr(node, 'data_source', None) is DataSource.STATIC:
                nodes.append(node)
        return nodes

    def _publish_static_writes(self, requested: dict, results: dict):
        """A STATIC point is never polled, so a write is the only time its value changes: record it and publish it."""
        values = {}
        for node in self._static_points(requested):
            reported = results.get(node.identifier)
            values[node.identifier] = reported if reported is not None else requested[node.identifier]
        if values:
            self.publish_push(values, raw=False)

    def publish_static(self, points):
        """Publish the current values of the STATIC points among ``points``: their configuration (Default Value) or
        whatever was last written. Called when a device is configured or reconfigured; nothing polls them."""
        values = {}
        for node in self._static_points(p.identifier for p in points):
            value = node.last_value
            if value is None:                                    # never set: the registry's Default Value, if any
                try:
                    value = getattr(self.get_register(node.identifier), 'default_value', None)
                except DriverInterfaceError:
                    value = None
                if value is not None:
                    node.last_value = value
            if value is not None:
                values[node.identifier] = value
        if values:
            self.publish_push(values, raw=False)

    def revert_point(self, topic, **kwargs):
        self.interface.revert_point(topic, **kwargs)

    def revert_all(self, **kwargs):
        self.interface.revert_all(**kwargs)

    def call(self, method_name, topics: list[str], *args, **kwargs):
        if not method_name in self.interface.interface_callable_methods or not hasattr(self.interface, method_name):
            err = (f'Called method {method_name} not found on interface "{self.interface.__class__.__name__}".'
                   f'Check if an expected plugin is not installed.')
            _log.warning(err)
            return {}, {topic: err for topic in topics}
        elif method_name in self.interface.excluded_from_callable_methods:
            err = f'Called method {method_name}, but it excluded from callable methods by configuration.'
            _log.warning(err)
            return {}, {topic: err for topic in topics}
        else:
            # A method of the interface (or a plugin method bound onto it): bound, so the interface is not passed again.
            return getattr(self.interface, method_name)(topics, *args, **kwargs)

    def publish_push(self, results, raw: bool = True):
        """Publish values that arrived without a poll (a change of value, an unsolicited response, a served peer's
        write), keyed by full point topic, and record them as the points' last values.

        Each topic is checked before anything is published: it must name a point of this remote. Anything else (an
        unknown topic, a device or other non-point node, a point of another remote) is logged and skipped, and one bad
        topic never prevents the rest of the batch from publishing. The proxy processes that push are authenticated,
        but this keeps a buggy or compromised proxy from setting values on points it does not serve. ``raw`` False
        means the values are already engineering values (a static point's) and no transform is applied.
        """
        et = self.equipment_model
        headers = publication_headers()
        results, errors = dict(results), {}
        if raw:
            self.transforms.apply(results, errors, self._last_value)
        for failed_point, failure_message in errors.items():
            _log.warning(f'{self.unique_id}: no value published for pushed {failed_point}: {failure_message}')
        derived_results, derived_errors = self.transforms.compute_dependents(results, self._last_value)
        results.update(derived_results)
        for failed_point, failure_message in derived_errors.items():
            _log.warning(f'{self.unique_id}: derived point {failed_point} not computed: {failure_message}')
        multi_depth_values, multi_depth_meta, multi_breadth_values, multi_breadth_meta = (
            defaultdict(dict), defaultdict(dict), defaultdict(dict), defaultdict(dict))
        for point_topic, value in results.items():
            point_node = et.get_node(point_topic)
            if point_node is None or not getattr(point_node, 'is_point', False):
                _log.warning(f'{self.unique_id}: ignoring a pushed value for {point_topic!r}, which is not a point.')
                continue
            try:
                owner = et.get_remote(point_topic)
            except Exception as e:                                   # a point without a device above it
                _log.warning(f'{self.unique_id}: ignoring a pushed value for {point_topic!r}: {e}')
                continue
            if owner is not self:
                _log.warning(f'{self.unique_id}: ignoring a pushed value for {point_topic!r}, a point of another remote.')
                continue
            try:
                point_depth_topic, point_breadth_topic = et.get_point_topics(point_topic)
                device_depth_topic, device_breadth_topic = et.get_device_topics(point_topic)
                if et.is_active(point_topic):
                    point_node.last_value = value
                    if (record := getattr(et, 'record_pushed_value', None)) is not None:
                        record(point_node)          # the platform driver keeps served points' values across restarts
                if et.is_published_single_depth(point_topic):
                    publish_wrapper(self.vip, point_depth_topic, headers, [value, point_node.meta_data])
                if et.is_published_single_breadth(point_topic):
                    publish_wrapper(self.vip, point_breadth_topic, headers, [value, point_node.meta_data])
                if et.is_published_multi_depth(point_topic):
                    point_name = point_topic.rsplit('/', 1)[-1]
                    multi_depth_values[device_depth_topic][point_name] = value
                    multi_depth_meta[device_depth_topic][point_name] = point_node.meta_data
                if et.is_published_multi_breadth(point_topic):
                    point_name = point_topic.rsplit('/', 1)[-1]
                    multi_breadth_values[device_breadth_topic][point_name] = value
                    multi_breadth_meta[device_breadth_topic][point_name] = point_node.meta_data
            except Exception as e:
                _log.warning(f'{self.unique_id}: unable to publish a pushed value for {point_topic!r}: {e}')
        if multi_depth_values:
            for device_topic in multi_depth_values:
                publish_wrapper(self.vip, f'{device_topic}/multi', headers,
                                      [multi_depth_values[device_topic], multi_depth_meta[device_topic]])
        if multi_breadth_values:
            for device_topic in multi_breadth_values:
                publish_wrapper(self.vip, f'{device_topic}/multi', headers,
                                [multi_breadth_values[device_topic], multi_breadth_meta[device_topic]])

    def add_equipment(self, device_node):
        # TODO: Is logic needed for scheduling or any other purpose on adding equipment to this remote?
        #_log.debug(f'IN ADD EQUIPMENT, with device_node: {device_node.identifier}')
        # Only the device's own points: points(...) would also return those of any device nested beneath this topic.
        self.add_registers([p.config for p in self.equipment_model.device_points(device_node.identifier)],
                           device_node.identifier)
        self.equipment.add(device_node)
        # What the device said about itself at registration (FC43, a BACnet device object ...) is device metadata.
        identification = getattr(self.interface, 'device_identification', None)
        if identification and hasattr(device_node, 'meta_data'):
            device_node.meta_data = {**(device_node.meta_data or {}), 'device_identification': identification}

    def health_report(self) -> dict:
        """This remote's health: what the framework observed, plus whatever the interface's ``health()`` adds."""
        protocol = None
        health = getattr(self.interface, 'health', None)
        if callable(health):
            try:
                protocol = health()
            except Exception as e:
                protocol = {'error': f'protocol health unavailable: {e}'}
        report = self.health.as_dict(protocol)
        report['remote'] = str(self.unique_id)
        report['devices'] = sorted(getattr(node, 'identifier', str(node)) for node in self.equipment)
        return report

    @property
    def point_set(self):
        return {point for equip in self.equipment
                for point in self.equipment_model.device_points(equip.identifier)}
