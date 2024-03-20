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

import datetime
import gevent
import logging
import random
import traceback

from typing import cast
from weakref import WeakSet

from volttron.client.messaging import headers as headers_mod
from volttron.client.messaging.topics import (
    DEVICES_PATH,
    DEVICES_VALUE,
    DRIVER_TOPIC_ALL,
    DRIVER_TOPIC_BASE,
)
from volttron.client.vip.agent import BasicAgent, Core
from volttron.client.vip.agent.errors import Again, VIPError
from volttron.utils import format_timestamp, get_aware_utc_now, setup_logging

from volttron.driver.base.driver_locks import publish_lock
from volttron.driver.base.utils import PublishFormat, setup_publishes
from volttron.driver.base.interfaces import BaseInterface

setup_logging()
_log = logging.getLogger(__name__)


class DriverAgent(BasicAgent):

    def __init__(self,
                 parent,
                 config,
                 # TODO: Make polling and grouping parameters all optional and/or keyword only.
                 # time_slot, # Deprecated: Use poll scheduler.
                 # driver_scrape_interval, # Deprecated: Use poll scheduler.
                 # device_path,  # TODO: Does this parameter make sense in context of driver service?
                 # group, # Deprecated: Use poll scheduler.
                 # group_offset_interval, # Deprecated: Use poll scheduler.
                 # TODO: Deprecated. We can get these from parent.config.
                 # default_publish_depth_first_all=False,
                 # default_publish_breadth_first_all=False,
                 # default_publish_depth_first=False,
                 # default_publish_breadth_first=False,
                 # New parameters to support Driver Service:
                 unique_id=None,
                 ** kwargs):
        super(DriverAgent, self).__init__(**kwargs)
        self.config = config
        # self.device_path = device_path
        self.parent = parent
        self.unique_id = unique_id
        self.vip = parent.vip

        self.device_name = ''
        self.equipment = WeakSet()
        self.heart_beat_value = 0

        # TODO: Replace Nones with setup_publishes when DriverAgentConfig is done in Pydantic
        self.breadth_first_publishes, self.depth_first_publishes = None, None  # setup_publishes(self.config, self.parent.config)

        ######## TODO: Handle this block with Poll Scheduler. #############
        # try:
        #     interval = int(config.get("interval", 60))
        #     if interval < 1.0:
        #         raise ValueError
        # except ValueError:
        #     _log.warning(f"Invalid device scrape interval {config.get('interval')}. Defaulting to 60 seconds.")
        #     interval = 60
        #
        # self.interval = interval
        ######### END BLOCK ##############

        self.periodic_read_event = None

        ################ TODO: Handle this block with PollScheduler instead. #######################
    #     #self.update_scrape_schedule(time_slot, driver_scrape_interval, group, group_offset_interval)
    #
    # def update_scrape_schedule(self, time_slot, driver_scrape_interval, group, group_offset_interval):
    #     self.time_slot_offset = (time_slot * driver_scrape_interval) + (group * group_offset_interval)
    #     self.time_slot = time_slot
    #     self.group = group
    #
    #     _log.debug(f"{self.device_path} group: {group}, time_slot: {time_slot}, offset: {self.time_slot_offset}")
    #     if self.time_slot_offset >= self.interval:
    #         _log.warning("Scrape offset exceeds interval. Required adjustment will cause scrapes to double up with"
    #                      " other devices.")
    #         while self.time_slot_offset >= self.interval:
    #             self.time_slot_offset -= self.interval
    #
    #     self._setup_periodic()
    #
    #
    #
    # def _setup_periodic(self, initial_setup=False):
    #     if self.periodic_read_event:
    #         self.periodic_read_event.cancel()
    #     elif not initial_setup:  # Only create initial periodic if this is called by startup.
    #         return
    #     next_periodic_read = self.find_starting_datetime(get_aware_utc_now())
    #     self.periodic_read_event = self.core.schedule(next_periodic_read, self.periodic_read, next_periodic_read)

    ########################## END BLOCK ###################################################

    def find_starting_datetime(self, now):
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_from_midnight = (now - midnight).total_seconds()
        interval = self.interval

        offset = seconds_from_midnight % interval

        if not offset:
            return now

        previous_in_seconds = seconds_from_midnight - offset
        next_in_seconds = previous_in_seconds + interval

        from_midnight = datetime.timedelta(seconds=next_in_seconds)
        return midnight + from_midnight + datetime.timedelta(seconds=self.time_slot_offset)

    def get_interface(self, driver_type: str, driver_config: dict,
                      registry_config: list) -> BaseInterface:
        """Returns an instance of the interface

        :param driver_type: The name of the driver
        :type driver_type: str
        :param driver_config: The configuration of the driver
        :type driver_config: dict
        :param registry_config: A list of registry points reprsented as dictionaries
        :type registry_config: list

        :return: Returns an instance of a Driver that is a subclass of BaseInterface
        :rtype: BaseInterface

        :raises ValueError: Raises ValueError if no subclasses are found.
        """
        klass = BaseInterface.get_interface_subclass(driver_type)
        _log.debug(f"Instantiating driver: {klass}")
        interface = klass(vip=self.vip, core=self.core) #, device_path=self.device_path)

        _log.debug(f"Configuring driver with this configuration: {driver_config}")
        interface.configure(driver_config, registry_config)
        return cast(BaseInterface, interface)

    @Core.receiver('onstart')
    def starting(self, sender, **kwargs):
        self.setup_device()
        # self._setup_periodic(initial_setup=True) # TODO: Handle with Poll Scheduler.
        # TODO: If these paths are still useful, add instance variables in the constructor.
        # self.all_path_depth, self.all_path_breadth = self.get_paths_for_point(DRIVER_TOPIC_ALL)

    def setup_device(self):
        # TODO: Make Pydantic configurations for this.
        config = self.config
        driver_config = config.get('remote_config', config.get('driver_config', {}))
        driver_type = config["driver_type"]
        registry_config = config.get("registry_config")
        self.heart_beat_point = config.get("heart_beat_point")

        try:
            self.interface = self.get_interface(driver_type, driver_config, registry_config)
        except ValueError as e:
            _log.error(f"Failed to setup device: {e}")
            raise e

        self.meta_data = {}

        for point in self.interface.get_register_names():
            register = self.interface.get_register_by_name(point)
            if register.register_type == 'bit':
                ts_type = 'boolean'
            else:
                if register.python_type is int:
                    ts_type = 'integer'
                elif register.python_type is float:
                    ts_type = 'float'
                elif register.python_type is str:
                    ts_type = 'string'

            self.meta_data[point] = {
                'units': register.get_units(),
                'type': ts_type,
                'tz': config.get('timezone', '')
            }

        # TODO: Commented because topic information needs to come from the equipment node. (Not unique here, anymore).
        # self.base_topic = DEVICES_VALUE(campus='',
        #                                 building='',
        #                                 unit='',
        #                                 path=self.device_path,
        #                                 point=None)
        #
        # self.device_name = DEVICES_PATH(base='',
        #                                 node='',
        #                                 campus='',
        #                                 building='',
        #                                 unit='',
        #                                 path=self.device_path,
        #                                 point='')

        # self.parent.device_startup_callback(self.device_name, self)

    # def periodic_read(self, now):
    #     #we not use self.core.schedule to prevent drift.
    #     next_scrape_time = now + datetime.timedelta(seconds=self.interval)
    #     # Sanity check now.
    #     # This is specifically for when this is running in a VM that gets
    #     # suspended and then resumed.
    #     # If we don't make this check a resumed VM will publish one event
    #     # per minute of
    #     # time the VM was suspended for.
    #     # TODO: How much of this will be handled by Poll Scheduler and how much remains here?
    #     test_now = get_aware_utc_now()
    #     if test_now - next_scrape_time > datetime.timedelta(seconds=self.interval):
    #         next_scrape_time = self.find_starting_datetime(test_now)
    #
    #     _log.debug("{} next scrape scheduled: {}".format(self.device_path, next_scrape_time))
    #
    #     self.periodic_read_event = self.core.schedule(next_scrape_time, self.periodic_read,
    #                                                   next_scrape_time)
    #
    #     _log.debug("scraping device: " + self.device_name)
    #
    #     if self.parent.scalability_test:
    #         self.parent.scalability_test.poll_starting(self.device_name)
    #
    #     try:
    #         # TODO: Will need to be changed to get_multiple().
    #         results = self.interface.scrape_all()
    #         register_names = self.interface.get_register_names_view()
    #         for point in (register_names - results.keys()):
    #             depth_first_topic = self.base_topic(point=point)
    #             _log.error("Failed to scrape point: " + depth_first_topic)
    #     except (Exception, gevent.Timeout) as ex:
    #         tb = traceback.format_exc()
    #         _log.error('Failed to scrape ' + self.device_name + ':\n' + tb)
    #         return
    #
    #     # XXX: Does a warning need to be printed?
    #     if not results:
    #         return
    #
    #     utcnow = get_aware_utc_now()
    #     utcnow_string = format_timestamp(utcnow)
    #     sync_timestamp = format_timestamp(now - datetime.timedelta(seconds=self.time_slot_offset))
    #
    #     headers = {
    #         headers_mod.DATE: utcnow_string,
    #         headers_mod.TIMESTAMP: utcnow_string,
    #         headers_mod.SYNC_TIMESTAMP: sync_timestamp
    #     }
    #
    #     if PublishFormat.Single in self.depth_first_publishes or PublishFormat.Single in self.breadth_first_publishes:
    #         for point, value in results.items():
    #             depth_first_topic, breadth_first_topic = self.get_paths_for_point(point)
    #             message = [value, self.meta_data[point]]
    #
    #             if PublishFormat.Single in self.depth_first_publishes:
    #                 self._publish_wrapper(depth_first_topic, headers=headers, message=message)
    #
    #             if PublishFormat.Single in self.breadth_first_publishes:
    #                 self._publish_wrapper(breadth_first_topic, headers=headers, message=message)
    #
    #     # TODO: Need to find any-type topics here:
    #     message = [results, self.meta_data]
    #     if PublishFormat.Any in self.depth_first_publishes:
    #         self._publish_wrapper(self.any_path_depth, headers=headers, message=message)
    #
    #     if PublishFormat.Any in self.breadth_first_publishes:
    #         self._publish_wrapper(self.any_path_breadth, headers=headers, message=message)
    #
    #     # TODO: Include last values in message before all publishes.
    #     if PublishFormat.All in self.depth_first_publishes:
    #         self._publish_wrapper(self.all_path_depth, headers=headers, message=message)
    #
    #     if PublishFormat.All in self.breadth_first_publishes:
    #         self._publish_wrapper(self.all_path_breadth, headers=headers, message=message)
    #
    #     if self.parent.scalability_test:
    #         self.parent.scalability_test.poll_ending(self.device_name)

    def _publish_wrapper(self, topic, headers, message):
        while True:
            try:
                with publish_lock():
                    _log.debug("publishing: " + topic)
                    self.vip.pubsub.publish('pubsub', topic, headers=headers,
                                            message=message).get(timeout=10.0)

                    _log.debug("finish publishing: " + topic)
            except gevent.Timeout:
                _log.warning("Did not receive confirmation of publish to " + topic)
                break
            except Again:
                _log.warning("publish delayed: " + topic + " pubsub is busy")
                gevent.sleep(random.random())
            except VIPError as ex:
                _log.warning("driver failed to publish " + topic + ": " + str(ex))
                break
            else:
                break

    def heart_beat(self):
        if self.heart_beat_point is None:
            return

        self.heart_beat_value = int(not bool(self.heart_beat_value))

        _log.debug("sending heartbeat: " + self.device_name + ' ' + str(self.heart_beat_value))

        self.set_point(self.heart_beat_point, self.heart_beat_value)

    # TODO: This is problematic now, since:
    #  (a) topics are not unique in remote,
    #  (b) need to handle any publishes
    #  (c) breadth topics should by default have "points/" as root topic.
    #  (d) topic roots should be configurable at the agent level.
    # def get_paths_for_point(self, point):
    #     depth_first = self.base_topic(point=point)
    #
    #     parts = depth_first.split('/')
    #     breadth_first_parts = parts[1:]
    #     breadth_first_parts.reverse()
    #     breadth_first_parts = [DRIVER_TOPIC_BASE] + breadth_first_parts
    #     breadth_first = '/'.join(breadth_first_parts)
    #
    #     return depth_first, breadth_first

    def get_point(self, point_name, **kwargs):
        return self.interface.get_point(point_name, **kwargs)

    def set_point(self, point_name, value, **kwargs):
        return self.interface.set_point(point_name, value, **kwargs)

    def scrape_all(self):
        return self.interface.scrape_all()

    def get_multiple_points(self, point_names, **kwargs):
        return self.interface.get_multiple_points(self.device_name, point_names, **kwargs)

    def set_multiple_points(self, point_names_values, **kwargs):
        return self.interface.set_multiple_points(self.device_name, point_names_values, **kwargs)

    def revert_point(self, point_name, **kwargs):
        self.interface.revert_point(point_name, **kwargs)

    def revert_all(self, **kwargs):
        self.interface.revert_all(**kwargs)

    def publish_cov_value(self, point_name, point_values):
        """
        Called in the platform driver agent to publish a cov from a point
        :param point_name: point which sent COV notifications
        :param point_values: COV point values
        """
        utcnow = get_aware_utc_now()
        utcnow_string = format_timestamp(utcnow)
        headers = {
            headers_mod.DATE: utcnow_string,
            headers_mod.TIMESTAMP: utcnow_string,
        }
        for point, value in point_values.items():
            results = {point_name: value}
            meta = {point_name: self.meta_data[point_name]}
            all_message = [results, meta]
            individual_point_message = [value, self.meta_data[point_name]]

            depth_first_topic, breadth_first_topic = self.get_paths_for_point(point_name)

            if self.publish_depth_first:
                self._publish_wrapper(depth_first_topic,
                                      headers=headers,
                                      message=individual_point_message)
            #
            if self.publish_breadth_first:
                self._publish_wrapper(breadth_first_topic,
                                      headers=headers,
                                      message=individual_point_message)

            if self.publish_depth_first_all:
                self._publish_wrapper(self.all_path_depth, headers=headers, message=all_message)

            if self.publish_breadth_first_all:
                self._publish_wrapper(self.all_path_breadth, headers=headers, message=all_message)

    ##### NEW METHODS TO SUPPORT DRIVER SERVICE #####
    def add_equipment(self, device_node):  # New addition, no conflict.
        # TODO: Is logic needed for scheduling or any other purpose on adding equipment to this remote?
        self.equipment.add(device_node)

    @property
    def point_set(self):
            return {point for equip in self.equipment for point in self.parent.equipment_tree.points(equip.identifier)}
