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
import traceback

from typing import cast
from weakref import WeakSet


from volttron.client.vip.agent import Agent, BasicAgent, Core
from volttron.utils import setup_logging

from volttron.driver.base.interfaces import BaseInterface
from volttron.driver.base.config import PointConfig, RemoteConfig
from .utils import publication_headers, publish_wrapper

setup_logging()
_log = logging.getLogger(__name__)


class DriverAgent(BasicAgent):

    def __init__(self, config: RemoteConfig, equipment_model, scalability_test, tz: str, unique_id: any,
                 vip: Agent.Subsystems, ** kwargs):
        super(DriverAgent, self).__init__(**kwargs)
        self.config: RemoteConfig = config
        self.equipment_model = equipment_model  # TODO: This should probably move out of the agent and into the base or a library.
        self.scalability_test = scalability_test  # TODO: If this is used from here, it should probably be in the base driver.
        self.tz: str = tz  # TODO: This won't update here if it is updated in the agent. Can it's use be moved out of here?
        self.unique_id: any = unique_id
        self.vip: Agent.Subsystems = vip
        
        # State Variables
        self.equipment = WeakSet()
        self.heart_beat_value = 0
        self.interface = None
        self.publishers = {}

        try:
            # TODO: What happens if we have multiple device nodes on this remote?
            #  Are we losing all settings but the first?
            klass = BaseInterface.get_interface_subclass(self.config.driver_type)
            interface = klass(self.config)
            self.interface = cast(BaseInterface, interface)
        except ValueError as e:
            _log.error(f"Failed to setup device: {e}")
            raise e

    def add_registers(self, registry_config: list[PointConfig], base_topic: str):
        """
        Configure a set of registers on this remote.

        :param registry_config: A list of registry points represented as PointConfigs
        :param base_topic: The portion of the topic shared by all points in this registry.
        """
        for register_config in registry_config:
            register = self.interface.create_register(register_config)
            self.interface.insert_register(register, base_topic)
        self.interface.finalize_setup(initial_setup=True)

        for point_name in self.interface.get_register_names():
            register = self.interface.get_register_by_name(point_name)
            point = self.equipment_model.get_node(point_name)
            # TODO: It might be more reasonable to either have the register be aware of the type mappings or have a
            #  type-mapping function separately. This is rather limiting. What is "ts" anyway? TypeScript?
            if register.register_type == 'bit':
                ts_type = 'boolean'
            else:
                if register.python_type is int:
                    ts_type = 'integer'
                elif register.python_type is float:
                    ts_type = 'float'
                elif register.python_type is str:
                    ts_type = 'string'
            # TODO: Why is there not an else here? ts_type may be undefined.
            # TODO: meta_data may belong in the PointNode object. This function could take points instead of their
            #  configs and pack the data into the PointNode instead of a separate dictionary in this class.
            point.meta_data = {
                'units': register.get_units(),
                'type': ts_type,
                'tz': self.tz
            }

    @Core.receiver('onstart')
    def on_start(self, _, **__):
        """Calls on_start on the interface to allow any on_start methods there.
            The BaseInterface on_start receives the vip and core objects.
        :raises ValueError: Raises ValueError if no subclasses are found.
        """
        self.interface.on_start(self.core, self.vip)

    def poll_data(self, current_points, publish_setup):
        _log.debug(f'@@@@@ Polling: {self.unique_id}')
        if self.scalability_test:  # TODO: Update scalability testing.
            self.scalability_test.poll_starting(self.unique_id)
        try:
            results, errors = self.interface.get_multiple_points(current_points.keys())
            for failed_point, failure_message in errors.items():
                _log.warning(f'Failed to poll {failed_point}: {failure_message}')
            if results:
                for topic, value in results.items():
                    try:
                        current_points[topic].last_value = value
                    except KeyError as e:
                        _log.warning(f'Failed to set last value: "{e}". Point may no longer be found in EquipmentTree.')
                self.publish(results, publish_setup)
            return True  # TODO: There could really be better logic in the method to measure success.
        except (Exception, gevent.Timeout) as e:
            _log.error(f'Exception while polling {self.unique_id}: {e}')
            if self.config.debug:
                # TODO: Add an RPC to turn on debugging for individual remotes. Maybe for nodes as well?
                tb = traceback.format_exc()
                _log.error(tb)
            return False
        finally:
            if self.scalability_test:
                self.scalability_test.poll_ending(self.unique_id)

    # noinspection DuplicatedCode
    def publish(self, results, publish_setup):
        headers = publication_headers()
        # TODO: Should probably wrap in try block(s). One for each loop, so it catches failure with single iterations?
        for point_topic in publish_setup['single_depth']:
            publish_wrapper(self.vip, point_topic, headers=headers, message=[
                results[point_topic], self.equipment_model.get_node(point_topic).meta_data
            ])
        for point_topic, publish_topic in publish_setup['single_breadth']:
            publish_wrapper(self.vip, publish_topic, headers=headers, message=[
                results[point_topic], self.equipment_model.get_node(point_topic).meta_data
            ])
        for device_topic, points in publish_setup['multi_depth'].items():
            publish_wrapper(self.vip, f'{device_topic}/multi', headers=headers, message=[
                {point.rsplit('/', 1)[-1]: results[point] for point in points if point in results},
                {point.rsplit('/', 1)[-1]: self.equipment_model.get_node(point).meta_data for point in points}
            ])
        for (device_topic, publish_topic), points in publish_setup['multi_breadth'].items():
            publish_wrapper(self.vip, f'{publish_topic}/multi', headers=headers, message=[
                {point.rsplit('/', 1)[-1]: results[point] for point in points if point in results},
                {point.rsplit('/', 1)[-1]: self.equipment_model.get_node(point).meta_data for point in points}
            ])

    def heart_beat(self):
        if self.config.heart_beat_point is None:
            return
        self.heart_beat_value = int(not bool(self.heart_beat_value))
        _log.info(f'sending heartbeat: {self.unique_id} --- {str(self.heart_beat_value)}')
        self.set_point(self.config.heart_beat_point, self.heart_beat_value)

    def get_point(self, topic, **kwargs):
        return self.interface.get_point(topic, **kwargs)

    def set_point(self, topic, value, **kwargs):
        return self.interface.set_point(topic, value, **kwargs)

    def scrape_all(self):
        return self.interface.scrape_all()

    def get_multiple_points(self, topics, **kwargs):
        return self.interface.get_multiple_points(topics, **kwargs)

    def set_multiple_points(self, topics_values, **kwargs):
        return self.interface.set_multiple_points(topics_values, **kwargs)

    def revert_point(self, topic, **kwargs):
        self.interface.revert_point(topic, **kwargs)

    def revert_all(self, **kwargs):
        self.interface.revert_all(**kwargs)

    def publish_cov_value(self, point_name, point_values):
        """
        Called in the platform driver agent to publish a cov from a point
        :param point_name: point which sent COV notifications
        :param point_values: COV point values
        """
        et = self.equipment_model
        headers = publication_headers()
        for point, value in point_values.items(): # TODO: How is point different from point_name?
            # TODO: Several of these things can be outside loop if the point in the loop isn't important.
            point_depth_topic, point_breadth_topic = et.get_point_topics(point_name)
            device_depth_topic, device_breadth_topic = et.get_device_topics(point_name)
            point_node = self.equipment_model.get_node(point_name)
            if et.is_published_single_depth(point_name):
                publish_wrapper(self.vip, point_depth_topic, headers, [value, point_node.meta_data])
            if et.is_published_single_breadth(point_name):
                publish_wrapper(self.vip, point_breadth_topic, headers, [value, point_node.meta_data])
            if et.is_published_multi_depth(point_name) or et.is_published_all_depth(point_name):
                publish_wrapper(self.vip, device_depth_topic, headers,
                                      [{point_name: value}, {point_name: point_node.meta_data}])
            if et.is_published_multi_breadth(point_name) or et.is_published_all_breadth(point_name):
                publish_wrapper(self.vip, device_breadth_topic, headers,
                                      [{point_name: value}, {point_name: point_node.meta_data}])

    def add_equipment(self, device_node):
        # TODO: Is logic needed for scheduling or any other purpose on adding equipment to this remote?
        self.add_registers([p.config for p in self.equipment_model.points(device_node.identifier)],
                           device_node.identifier)
        self.equipment.add(device_node)

    @property
    def point_set(self):
            return {point for equip in self.equipment for point in self.equipment_model.points(equip.identifier)}
