"""Remote health records, one bad registry row never taking a device down, and interface callables through call()."""
import logging

from types import SimpleNamespace
from unittest import mock

import pytest

from volttron.driver.base.config import PointConfig, accepted_columns
from volttron.driver.base.health import RemoteHealth, STATUS_BAD, STATUS_GOOD, STATUS_UNKNOWN

from test_transforms import DEVICE, FakeInterface, PollSet, RowConfig, T, row, world, fake_module  # noqa: F401


class TestRemoteHealth:
    def test_status_follows_polls(self):
        changes = []
        health = RemoteHealth('r', on_change=lambda h: changes.append(h.status))
        assert health.status == STATUS_UNKNOWN
        health.record_poll({'a': 1}, {'b': 'nope'}, 0.2)
        assert health.status == STATUS_GOOD and health.point_errors == {'b': 'nope'} and health.last_poll_duration == 0.2
        health.record_poll({}, {'a': 'timeout', 'b': 'timeout'})
        health.record_poll({}, {}, exception=RuntimeError('boom'))
        assert health.status == STATUS_BAD and health.consecutive_failures == 2 and health.poll_failures == 2
        assert health.last_error == 'RuntimeError: boom' and changes == [STATUS_GOOD, STATUS_BAD]
        health.record_poll({'a': 2}, {})
        assert changes == [STATUS_GOOD, STATUS_BAD, STATUS_GOOD] and health.consecutive_failures == 0

    def test_writes_events_and_report(self):
        health = RemoteHealth('r')
        health.record_write({}); health.record_write({'x': 'read only'})
        health.note('proxy lost')
        report = health.as_dict({'protocol': 'modbus'})
        assert report['writes'] == 2 and report['write_failures'] == 1 and report['last_write_error'] == 'read only'
        assert report['events'][0]['event'] == 'proxy lost' and report['protocol'] == {'protocol': 'modbus'}
        assert report['status'] == STATUS_UNKNOWN and report['last_success'] is None

    def test_driver_agent_records_polls_and_writes(self, world):
        agent, tree = world
        agent.build([row('A', type='int', writable=True), row('B', type='int')])
        with mock.patch('volttron.driver.base.driver.publish_wrapper'):
            agent.poll_data(PollSet(tree, [T('A'), T('B')]))
        assert agent.health.status == STATUS_GOOD and agent.health.polls == 1
        agent.set_point(T('A'), 3)
        with pytest.raises(Exception):
            agent.set_point(T('B'), 3)                                   # read only
        assert agent.health.writes == 2 and agent.health.write_failures == 1
        agent.interface.raw.clear()                                       # the device stops answering
        with mock.patch('volttron.driver.base.driver.publish_wrapper'):
            agent.poll_data(PollSet(tree, [T('A')]))
        assert agent.health.status == STATUS_BAD and 'not configured' in agent.health.last_error
        report = agent.health_report()
        assert report['remote'] == 'faketf' and report['status'] == STATUS_BAD and 'protocol' not in report

    def test_interface_health_is_merged(self, world):
        agent, tree = world
        agent.build([row('A', type='int')])
        agent.interface.health = lambda: {'protocol': 'fake', 'connected': True}
        assert agent.health_report()['protocol'] == {'protocol': 'fake', 'connected': True}
        agent.interface.health = lambda: (_ for _ in ()).throw(RuntimeError('down'))
        assert 'down' in agent.health_report()['protocol']['error']


class TestBadRows:
    def test_a_bad_row_is_dropped_and_the_rest_configure(self, world, caplog):
        agent, tree = world
        removed = []
        tree.remove_segment = lambda topic: removed.append(topic) or tree.nodes.pop(topic)
        bad = RowConfig(volttron_point_name='Bad', type='nosuch')
        rows = [row('A', type='int'), bad, row('B', type='float')]
        tree.add_points(DEVICE, rows)
        with caplog.at_level(logging.ERROR):
            failed = agent.add_registers(rows, DEVICE)
        assert failed == [T('Bad')] and removed == [T('Bad')] and T('Bad') not in tree.nodes
        assert set(agent.interface.point_map) == {T('A'), T('B')} and 'point devices/campus/building/meter/Bad is not configured' in caplog.text

    def test_device_identification_becomes_device_metadata(self, world):
        agent, tree = world
        agent.interface.device_identification = {'VendorName': 'ACME'}

        class Node:
            identifier, meta_data = DEVICE, {'units': 'x'}
        node = Node()
        tree.add_points(DEVICE, [row('A', type='int')])
        agent.add_equipment(node)
        assert node.meta_data == {'units': 'x', 'device_identification': {'VendorName': 'ACME'}}


class TestCallables:
    def test_call_passes_topics_to_a_bound_method(self, world):
        agent, tree = world
        agent.build([row('A', type='int')])
        agent.interface.interface_callable_methods = {'describe'}
        agent.interface.describe = lambda topics, detail=False: ({t: f'point {detail}' for t in topics}, {})
        assert agent.call('describe', [T('A')], detail=True) == ({T('A'): 'point True'}, {})
        results, errors = agent.call('nosuch', [T('A')])
        assert results == {} and 'not found' in errors[T('A')]


def test_accepted_columns_lists_names_and_aliases():
    columns = accepted_columns(PointConfig)
    assert {'volttron_point_name', 'Volttron Point Name', 'transform', 'Transform', 'data_source', 'Data Source',
            'polling_group', 'group', 'Group', 'interval'} <= columns
