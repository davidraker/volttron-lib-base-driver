"""The Transform column: compiled by DriverAgent at configure time and applied around the interface at run time.

A fake interface keeps raw values in a dict; a fake equipment model holds point nodes with last values. Everything
the platform sees through DriverAgent is in engineering units, everything the interface sees is raw.
"""
import logging
import sys

from types import ModuleType, SimpleNamespace
from unittest import mock

import pytest

from pydantic import Field

from volttron.driver.base.config import DataSource, PointConfig, RemoteConfig
from volttron.driver.base.driver import DriverAgent
from volttron.driver.base.interfaces import BaseInterface, BaseRegister, BasicRevert, DriverInterfaceError
from volttron.driver.base.transforms import DerivedRegister, TransformConfigError, TransformTable, TransformValueError

pytest.importorskip('icm.transforms')

DEVICE = 'devices/campus/building/meter'
TYPES = {'int': int, 'float': float, 'bool': bool, 'str': str}


def T(name):
    return f'{DEVICE}/{name}'


class RowConfig(PointConfig):
    type: str = Field(default='int', alias='Type')
    starting_value: str = Field(default='', alias='Starting Value')


class FakeInterface(BasicRevert, BaseInterface):
    """Raw values in a dict; writes return what was stored."""
    REGISTER_CONFIG_CLASS = RowConfig
    default_config = {}

    def __init__(self, config, *args, **kwargs):
        BasicRevert.__init__(self)
        BaseInterface.__init__(self, config, *args, **kwargs)
        self.raw = {}
        self.writes = []

    def create_register(self, definition: RowConfig) -> BaseRegister:
        register = BaseRegister('byte', not definition.writable, definition.volttron_point_name, definition.units)
        register.python_type = TYPES[definition.type]
        return register

    def insert_register(self, register, base_topic):
        super().insert_register(register, base_topic)
        topic = '/'.join([base_topic, register.point_name])
        self.raw.setdefault(topic, register.python_type())

    def get_point(self, topic, **kwargs):
        return self.raw[topic]

    def _get_multiple_points(self, topics, **kwargs):
        results, errors = {}, {}
        for topic in topics:
            if topic in self.raw:
                results[topic] = self.raw[topic]
            else:
                errors[topic] = 'not configured'
        return results, errors

    def _set_point(self, topic, value):
        register = self.get_register_by_name(topic)
        if register.read_only:
            raise RuntimeError('read only')
        self.raw[topic] = register.python_type(value)
        self.writes.append((topic, self.raw[topic]))
        return self.raw[topic]


class FakeNode(SimpleNamespace):
    @property
    def data_source(self):
        return self.config.data_source


class FakeTree:
    """Enough of the platform driver's EquipmentTree for DriverAgent: nodes by topic, device membership, publishing."""

    def __init__(self):
        self.nodes = {}
        self.remote = None

    def add_points(self, device, configs):
        for config in configs:
            topic = '/'.join([device, config.volttron_point_name])
            self.nodes[topic] = FakeNode(identifier=topic, config=config, last_value=None, last_updated=None,
                                         is_point=True, meta_data={}, active=True)

    def get_node(self, topic):
        return self.nodes.get(topic)

    def device_points(self, device):
        return [n for t, n in self.nodes.items() if t.rsplit('/', 1)[0] == device]

    def is_active(self, topic):
        return True

    def get_remote(self, topic):
        return self.remote

    def get_point_topics(self, topic):
        return topic, 'breadth/' + topic

    def get_device_topics(self, topic):
        device = topic.rsplit('/', 1)[0]
        return device, 'breadth/' + device

    def is_published_single_depth(self, topic):
        return True

    def is_published_single_breadth(self, topic):
        return False

    def is_published_multi_depth(self, topic):
        return True

    def is_published_multi_breadth(self, topic):
        return False


def row(name, transform='', writable=False, type='float', data_source='short_poll', units=''):
    return RowConfig(volttron_point_name=name, transform=transform, writable=writable, type=type,
                     data_source=data_source, units=units)


@pytest.fixture
def fake_module(monkeypatch):
    module = ModuleType('volttron.driver.interfaces.faketf.faketf')
    FakeInterface.__module__ = module.__name__
    module.FakeInterface = FakeInterface
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return module


@pytest.fixture
def world(fake_module):
    """(agent, tree): build(rows) configures a device; the agent's interface is the FakeInterface."""
    tree = FakeTree()
    agent = DriverAgent(RemoteConfig(driver_type='faketf'), core=mock.Mock(), equipment_model=tree,
                        scalability_test=None, tz='UTC', unique_id='faketf', vip=mock.Mock())
    tree.remote = agent

    def build(rows, device=DEVICE):
        tree.add_points(device, rows)
        agent.add_registers(rows, device)
        return agent

    agent.build = build
    return agent, tree


class PollSet(SimpleNamespace):
    """What publish_poll reads from a PollSet."""

    def __init__(self, tree, topics):
        super().__init__(points={t: tree.nodes[t] for t in topics}, single_depth=set(topics), single_breadth=set(),
                         multi_depth={DEVICE: set(topics)}, multi_breadth={})


def publishes(publish):
    """{topic: message} of every publish_wrapper call."""
    return {call.args[1]: call.kwargs.get('message', call.args[3] if len(call.args) > 3 else None)
            for call in publish.call_args_list}


# ---- reads -----------------------------------------------------------------------------------------------------------

class TestReads:
    def test_poll_publishes_and_stores_engineering_values(self, world):
        agent, tree = world
        agent.build([row('Volts', 'scale(0.1)'), row('Plain')])
        agent.interface.raw[T('Volts')] = 2405
        agent.interface.raw[T('Plain')] = 7.0
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            assert agent.poll_data(PollSet(tree, [T('Volts'), T('Plain')]))
        published = publishes(publish)
        assert published[T('Volts')][0] == 240.5 and published[T('Plain')][0] == 7.0
        assert published[f'{DEVICE}/multi'][0] == {'Volts': 240.5, 'Plain': 7.0}
        assert tree.nodes[T('Volts')].last_value == 240.5
        assert agent.interface.raw[T('Volts')] == 2405                           # the interface never sees 240.5

    def test_rpc_get_point_and_get_multiple_points(self, world):
        agent, tree = world
        agent.build([row('Volts', 'transform(scale(0.1), add(-40))')])
        agent.interface.raw[T('Volts')] = 500
        assert agent.get_point(T('Volts')) == 10.0
        results, errors = agent.get_multiple_points([T('Volts')])
        assert results == {T('Volts'): 10.0} and errors == {}

    def test_read_options_bypass_the_transform(self, world):
        """Options select something other than the point's value (another BACnet property): the interface's answer stands."""
        agent, tree = world
        agent.build([row('Volts', 'scale(0.1)')])
        agent.interface.raw[T('Volts')] = 500
        assert agent.get_point(T('Volts'), property='units') == 500
        assert agent.get_multiple_points([T('Volts')], property='units')[0] == {T('Volts'): 500}

    def test_push_applies_the_forward_transform(self, world):
        agent, tree = world
        agent.build([row('Volts', 'scale(0.1)')])
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            agent.publish_push({T('Volts'): 1234})
        assert publishes(publish)[T('Volts')][0] == 123.4 and tree.nodes[T('Volts')].last_value == 123.4

    def test_metadata_type_follows_the_transform(self, world):
        agent, tree = world
        agent.build([row('Volts', 'scale(0.1)', type='int'), row('On', 'eq(1)', type='int'), row('Raw', type='int'),
                     row('Count', 'scale_int(10)', type='float')])
        types = {name: tree.nodes[T(name)].meta_data['type'] for name in ('Volts', 'On', 'Raw', 'Count')}
        assert types == {'Volts': 'float', 'On': 'boolean', 'Raw': 'integer', 'Count': 'integer'}

    def test_a_failing_transform_becomes_an_error_not_a_value(self, world, caplog):
        agent, tree = world
        agent.build([row('Ratio', 'divide_reg(Den)'), row('Den', type='int')])
        agent.interface.raw.update({T('Ratio'): 10.0, T('Den'): 0})
        with caplog.at_level(logging.WARNING):
            results, errors = agent.get_multiple_points([T('Ratio'), T('Den')])
        assert results == {T('Den'): 0} and 'produced no value' in errors[T('Ratio')]
        assert tree.nodes[T('Ratio')].last_value is None


# ---- writes ----------------------------------------------------------------------------------------------------------

class TestWrites:
    def test_set_point_inverts_and_reports_the_round_trip(self, world):
        agent, tree = world
        agent.build([row('Setpoint', 'transform(scale(0.1), add(-40))', writable=True, type='int')])
        assert agent.set_point(T('Setpoint'), 10.0) == 10.0
        assert agent.interface.raw[T('Setpoint')] == 500
        agent.build([row('Rounded', 'scale_int(10)', writable=True, type='int')])
        assert agent.set_point(T('Rounded'), 2.57) == 0                          # lossy: int(2.57 / 10) is 0, times 10

    def test_set_multiple_points(self, world):
        agent, tree = world
        agent.build([row('A', 'scale(0.1)', writable=True, type='int'), row('B', writable=True, type='int'),
                     row('C', 'absolute()', writable=True, type='int')])
        results, errors = agent.set_multiple_points([(T('A'), 5.0), (T('B'), 3), (T('C'), 1)])
        assert results == {T('A'): 5.0, T('B'): 3}
        assert agent.interface.raw[T('A')] == 50 and agent.interface.raw[T('B')] == 3
        assert 'cannot be inverted' in errors[T('C')] and agent.interface.raw[T('C')] == 0

    def test_non_invertible_point_warns_at_configure_and_fails_at_set(self, world, caplog):
        agent, tree = world
        with caplog.at_level(logging.WARNING):
            agent.build([row('Mag', 'absolute()', writable=True)])
        assert "'absolute' has no inverse" in caplog.text
        with pytest.raises(DriverInterfaceError, match='cannot be inverted'):
            agent.set_point(T('Mag'), 3.0)
        assert agent.interface.writes == []

    def test_revert_stays_in_the_raw_domain(self, world):
        agent, tree = world
        agent.build([row('Setpoint', 'scale(0.1)', writable=True, type='int')])
        agent.interface.raw[T('Setpoint')] = 500
        agent.get_multiple_points([T('Setpoint')])                               # clean value recorded: raw 500
        agent.set_point(T('Setpoint'), 70.0)
        assert agent.interface.raw[T('Setpoint')] == 700
        agent.revert_point(T('Setpoint'))
        assert agent.interface.raw[T('Setpoint')] == 500 and agent.interface.writes[-1] == (T('Setpoint'), 500)

    def test_to_raw_for_served_seeding(self, world):
        agent, tree = world
        agent.build([row('Volts', 'scale(0.1)', type='int'), row('Plain', type='int')])
        assert agent.to_raw(T('Volts'), 24.5) == 245 and agent.to_raw(T('Plain'), 7) == 7


# ---- configure time --------------------------------------------------------------------------------------------------

class TestConfigure:
    @pytest.mark.parametrize('text, message', [
        ('nosuch(1)', 'unknown transform function'),
        ('scale(0.1), add(-40)', r'transform\(f1'),
        ('scale_reg(Missing)', 'not a point of this device'),
        ('scale_reg(volts)', 'ambiguous'),
        ('scale_reg(Volts)', 'cannot reference itself'),
    ])
    def test_errors_leave_the_point_without_a_transform(self, world, caplog, text, message):
        agent, tree = world
        with caplog.at_level(logging.ERROR):
            agent.build([row('Volts', text), RowConfig(volttron_point_name='A', reference_point_name='volts'),
                         RowConfig(volttron_point_name='B', reference_point_name='volts')])
        assert T('Volts') in agent.interface.point_map and T('Volts') not in agent.transforms
        assert any(message.replace('\\', '') in r.message or __import__('re').search(message, r.message)
                   for r in caplog.records if r.levelno == logging.ERROR)
        agent.interface.raw[T('Volts')] = 3.0
        assert agent.get_point(T('Volts')) == 3.0

    def test_reference_cycle_is_an_error(self, world, caplog):
        agent, tree = world
        with caplog.at_level(logging.ERROR):
            agent.build([row('A', 'scale_reg(B)'), row('B', 'scale_reg(A)')])
        assert T('A') in agent.transforms and T('B') not in agent.transforms and 'cycle' in caplog.text

    def test_reference_by_register_name_of_a_modbus_tk_map(self, world):
        """A shared map names its own registers; each device's registry picks the Volttron Point Names."""
        class MapRow(RowConfig):
            register_name: str = ''
        rows = [MapRow(volttron_point_name='kW sum', register_name='kw_sum', transform='scale_reg_pow_10(prog_power_scale)',
                       type='int'),
                MapRow(volttron_point_name='PPS', register_name='prog_power_scale', type='int'),
                RowConfig(volttron_point_name='Amps', reference_point_name='i_a', transform='scale_reg(pps_ref)', type='int'),
                RowConfig(volttron_point_name='Scale', reference_point_name='pps_ref', type='int')]
        agent, tree = world
        agent.build(rows)
        assert agent.transforms.get(T('kW sum')).references == {'prog_power_scale': T('PPS')}
        assert agent.transforms.get(T('Amps')).references == {'pps_ref': T('Scale')}
        agent.interface.raw.update({T('kW sum'): 1234, T('PPS'): -1, T('Amps'): 500, T('Scale'): 10})
        results, errors = agent.get_multiple_points([T('kW sum'), T('PPS'), T('Amps'), T('Scale')])
        assert results[T('kW sum')] == 123.4 and results[T('Amps')] == 50.0 and errors == {}
        assert agent.startup_read_topics == [T('PPS'), T('Scale')]

    def test_never_poll_reference_is_honoured_with_a_warning(self, world, caplog):
        agent, tree = world
        with caplog.at_level(logging.WARNING):
            agent.build([row('W', 'sunssf(W_SF)'), row('W_SF', type='int', data_source='never_poll')])
        assert 'never_poll' in caplog.text and T('W') in agent.transforms
        assert agent.startup_read_topics == []

    def test_empty_column_and_shared_text(self, world):
        agent, tree = world
        agent.build([row('A', ''), row('B', 'scale(0.1)'), row('C', 'scale(0.1)')])
        assert T('A') not in agent.transforms
        assert agent.transforms.get(T('B')).transform is agent.transforms.get(T('C')).transform   # cached by text

    def test_reconfiguring_a_point_replaces_or_drops_its_transform(self, world):
        agent, tree = world
        agent.build([row('A', 'scale(0.1)')])
        agent.add_register(row('A', 'scale(0.01)'), DEVICE)
        assert agent.transforms.get(T('A')).text == 'scale(0.01)'
        agent.add_register(row('A'), DEVICE)
        assert T('A') not in agent.transforms
        agent.remove_register(T('A'))
        assert T('A') not in agent.interface.point_map

    def test_without_the_library_the_column_is_a_configuration_error(self, monkeypatch):
        monkeypatch.setattr('volttron.driver.base.transforms.compile_value_transform', None)
        with pytest.raises(TransformConfigError, match='icm-lib-transforms'):
            TransformTable().configure(row('A', 'scale(0.1)'), DEVICE, {'A': row('A', 'scale(0.1)')})


# ---- scale factors and other references -----------------------------------------------------------------------------

class TestReferences:
    def rows(self):
        return [row('W', 'sunssf(W_SF)', type='int'), row('W_SF', type='int'), row('V', 'scale_reg(Div)', type='int'),
                row('Div', type='int', data_source='poll_once')]

    def test_factor_in_the_same_read_comes_first(self, world):
        agent, tree = world
        agent.build(self.rows())
        tree.nodes[T('W_SF')].last_value = -3                                   # stale; the read carries -1
        agent.interface.raw.update({T('W'): 1234, T('W_SF'): -1, T('V'): 2400, T('Div')
                                    : 10})
        results, errors = agent.get_multiple_points([T('W'), T('W_SF'), T('V'), T('Div')])
        assert results == {T('W'): 123.4, T('W_SF'): -1, T('V'): 240.0, T('Div'): 10} and errors == {}

    def test_factor_from_last_value_when_not_in_the_read(self, world):
        agent, tree = world
        agent.build(self.rows())
        tree.nodes[T('W_SF')].last_value = -2
        agent.interface.raw[T('W')] = 1234
        assert agent.get_point(T('W')) == 12.34
        assert agent.get_multiple_points([T('W')])[0] == {T('W'): 12.34}

    def test_hold_out_logs_once_and_clears(self, world, caplog):
        agent, tree = world
        agent.build(self.rows())
        agent.interface.raw[T('W')] = 1234
        with caplog.at_level(logging.INFO):
            for _ in range(3):
                results, errors = agent.get_multiple_points([T('W')])
                assert results == {} and 'W_SF' in errors[T('W')] and 'no value' in errors[T('W')]
            with pytest.raises(DriverInterfaceError, match='W_SF'):
                agent.get_point(T('W'))
            held = [r for r in caplog.records if 'No value is published' in r.message]
            assert len(held) == 1
            tree.nodes[T('W_SF')].last_value = 0
            assert agent.get_point(T('W')) == 1234.0
            assert any('produces a value again' in r.message for r in caplog.records)

    def test_sunssf_not_implemented_holds_the_point_out(self, world):
        agent, tree = world
        agent.build(self.rows())
        tree.nodes[T('W_SF')].last_value = -32768
        agent.interface.raw[T('W')] = 1234
        results, errors = agent.get_multiple_points([T('W')])
        assert results == {} and T('W') in errors

    def test_writes_invert_with_the_last_factor(self, world):
        agent, tree = world
        agent.build([row('W', 'sunssf(W_SF)', writable=True, type='int'), row('W_SF', type='int')])
        tree.nodes[T('W_SF')].last_value = -1
        assert agent.set_point(T('W'), 123.4) == 123.4 and agent.interface.raw[T('W')] == 1234
        tree.nodes[T('W_SF')].last_value = None
        with pytest.raises(DriverInterfaceError, match='W_SF'):
            agent.set_point(T('W'), 1.0)

    def test_startup_read_topics_are_the_polled_references(self, world):
        agent, tree = world
        agent.build(self.rows())
        assert agent.startup_read_topics == [T('Div'), T('W_SF')]

    def test_poll_without_the_factor_publishes_nothing_for_the_point(self, world, caplog):
        agent, tree = world
        agent.build(self.rows())
        agent.interface.raw[T('W')] = 1234
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish, caplog.at_level(logging.WARNING):
            agent.poll_data(PollSet(tree, [T('W')]))
        assert publish.call_count == 0 and 'Failed to poll' in caplog.text


# ---- derived points ---------------------------------------------------------------------------------------------------

class TestDerived:
    def rows(self):
        return [row('A', type='int'), row('B', type='int'), row('C', type='float'),
                row('Total', 'sum_of(A, B)', data_source='virtual', units='kW'),
                row('Mean', 'mean_of(A, C)', data_source='virtual'),
                row('Same', 'all_equal_of(A, B)', data_source='virtual')]

    def test_not_handed_to_the_interface(self, world):
        agent, tree = world
        agent.build(self.rows())
        assert T('Total') not in agent.interface.point_map and isinstance(agent.get_register(T('Total')), DerivedRegister)
        assert sorted(agent.get_register_names()) == sorted(T(n) for n in ('A', 'B', 'C', 'Total', 'Mean', 'Same'))

    def test_type_and_units_in_metadata(self, world):
        agent, tree = world
        agent.build(self.rows())
        meta = {n: tree.nodes[T(n)].meta_data for n in ('Total', 'Mean', 'Same')}
        assert meta['Total'] == {'units': 'kW', 'type': 'integer', 'tz': 'UTC'}        # widest of int, int
        assert meta['Mean']['type'] == 'float' and meta['Same']['type'] == 'boolean'   # declared by the functions
        agent.build([row('X', type='int'), row('Y', type='float'), row('Wide', 'max_of(X, Y)', data_source='virtual')])
        assert tree.nodes[T('Wide')].meta_data['type'] == 'float'

    def test_computed_and_published_with_the_reads_of_its_references(self, world):
        agent, tree = world
        agent.build(self.rows())
        agent.interface.raw.update({T('A'): 2, T('B'): 3, T('C'): 1.0})
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            agent.poll_data(PollSet(tree, [T('A'), T('B')]))
        published = publishes(publish)
        assert published[T('Total')][0] == 5 and published[T('Same')][0] is False
        # Mean reads A from this poll and C from its last value; C has none yet, so the mean is over A alone.
        assert published[f'{DEVICE}/multi'][0] == {'A': 2, 'B': 3, 'Total': 5, 'Same': False, 'Mean': 2.0}
        assert tree.nodes[T('Total')].last_value == 5 and tree.nodes[T('Mean')].last_value == 2.0
        tree.nodes[T('C')].last_value = 1.0
        assert agent.get_point(T('Mean')) == 1.5

    def test_rpc_get_from_last_values_and_write_rejected(self, world):
        agent, tree = world
        agent.build(self.rows())
        tree.nodes[T('A')].last_value, tree.nodes[T('B')].last_value = 4, 6
        assert agent.get_point(T('Total')) == 10
        agent.interface.raw[T('A')] = 5                                           # the same read comes first
        results, errors = agent.get_multiple_points([T('Total'), T('A')])
        assert results[T('Total')] == 11 and results[T('A')] == 5
        with pytest.raises(DriverInterfaceError, match='not writable'):
            agent.set_point(T('Total'), 1)
        assert agent.set_multiple_points([(T('Total'), 1)])[1] == {T('Total'): mock.ANY}

    def test_register_only_transform_needs_a_virtual_point(self, world, caplog):
        agent, tree = world
        with caplog.at_level(logging.ERROR):
            agent.build([row('A', type='int'), row('Total', 'sum_of(A)', data_source='short_poll')])
        assert 'virtual' in caplog.text and T('Total') in agent.interface.point_map and T('Total') not in agent.transforms

    def test_virtual_point_without_a_transform_is_set_by_the_platform(self, world):
        agent, tree = world
        agent.build([row('A', type='int'), row('Mode', data_source='virtual', writable=True, units='enum'),
                     row('Fixed', data_source='virtual')])
        assert T('Mode') not in agent.interface.point_map and agent.get_register(T('Mode')).read_only is False
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            assert agent.set_point(T('Mode'), 'eco') == 'eco'
        assert publishes(publish)[T('Mode')][0] == 'eco' and tree.nodes[T('Mode')].last_value == 'eco'
        assert agent.get_point(T('Mode')) == 'eco' and agent.get_multiple_points([T('Mode'), T('A')])[0][T('Mode')] == 'eco'
        agent.update_metadata(T('Mode'))
        assert tree.nodes[T('Mode')].meta_data['type'] == 'string'
        with pytest.raises(DriverInterfaceError, match='not writable'):
            agent.set_point(T('Fixed'), 1)

    def test_writable_bit_writes_through_to_the_word(self, world):
        agent, tree = world
        agent.build([row('Status', type='int', writable=True),
                     row('Fault', 'writable_bit(Status, 3)', data_source='virtual', writable=True),
                     row('Run', 'writable_bit(Status, 0, True)', data_source='virtual', writable=True)])
        agent.interface.raw[T('Status')] = 0b0001
        tree.nodes[T('Status')].last_value = 0b0001
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            assert agent.set_point(T('Fault'), True) is True
        assert agent.interface.raw[T('Status')] == 0b1001                  # the word was written, bit 3 set
        assert publishes(publish)[T('Fault')][0] is True and tree.nodes[T('Fault')].last_value is True
        agent.interface.raw[T('Status')] = 0b1111                          # changed on the device; last_value is stale
        with mock.patch('volttron.driver.base.driver.publish_wrapper'):
            agent.set_point(T('Run'), False)                               # read=True: the fresh word is used
        assert agent.interface.raw[T('Status')] == 0b1110
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            agent.poll_data(PollSet(tree, [T('Status')]))
        assert publishes(publish)[f'{DEVICE}/multi'][0] == {'Status': 14, 'Fault': True, 'Run': False}

    def test_value_of_extracts_bits_without_a_second_register(self, world):
        agent, tree = world
        agent.build([row('Status', type='int'), row('Fault', 'transform(value_of(Status), bit(3))', data_source='virtual'),
                     row('Alarm', 'transform(value_of(Status), bit(0))', data_source='virtual')])
        assert T('Fault') not in agent.interface.point_map and tree.nodes[T('Fault')].meta_data['type'] == 'boolean'
        agent.interface.raw[T('Status')] = 0b1001
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            agent.poll_data(PollSet(tree, [T('Status')]))
        assert publishes(publish)[f'{DEVICE}/multi'][0] == {'Status': 9, 'Fault': True, 'Alarm': True}

    def test_literal_arguments_and_push(self, world):
        agent, tree = world
        agent.build([row('A', type='int'), row('Both3', 'all_equal_of(3, A)', data_source='virtual')])
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            agent.publish_push({T('A'): 3})
        assert publishes(publish)[T('Both3')][0] is True


# ---- modbus_tk parity -------------------------------------------------------------------------------------------------

class TestModbusTkParity:
    """The modbus_tk register-map transforms through the framework column: the helpers' own expected values."""

    @pytest.mark.parametrize('text, raw, expected, written', [
        ('scale(0.001)', 2500, 2.5, 2500),
        ('scale(10)', 7, 70, 7),
        ('scale_int(0.1)', 255, 25, 250),
        ('scale_decimal_int_signed(0.001)', 1500, 1.5, 1500),
        ('mod10k(False)', (5 << 16) | 7, 50007, None),
        ('mod10k(True)', (5 << 16) | 7, 70005, None),
        ('mod10k64(False)', (1 << 48) | (2 << 32) | (3 << 16) | 4, 4 * 10000 ** 3 + 3 * 10000 ** 2 + 2 * 10000 + 1, None),
        ('mod10k48(True)', (1 << 48) | (2 << 32) | (3 << 16) | 4, 4 * 10000 ** 2 + 3 * 10000 + 2, None),
        ('mod10k48(False)', (1 << 48) | (2 << 32) | (3 << 16) | 4, 3 * 10000 ** 2 + 2 * 10000 + 1, None),
        ('no_op()', 42, 42, 42),
    ])
    def test_single_function_forms(self, world, text, raw, expected, written):
        agent, tree = world
        agent.build([row('P', text, writable=written is not None, type='int')])
        agent.interface.raw[T('P')] = raw
        assert agent.get_point(T('P')) == expected
        if written is not None:
            agent.set_point(T('P'), expected)
            assert agent.interface.raw[T('P')] == written

    def test_register_scaling(self, world):
        agent, tree = world
        agent.build([row('V', 'scale_reg(Div)', writable=True, type='int'), row('Div', type='int'),
                     row('P', 'scale_reg_pow_10(Exp)', writable=True, type='int'), row('Exp', type='int')])
        agent.interface.raw.update({T('V'): 2400, T('Div'): 10, T('P'): 1234, T('Exp'): -2})
        results, _ = agent.get_multiple_points([T('V'), T('Div'), T('P'), T('Exp')])
        assert results[T('V')] == 240.0 and results[T('P')] == 12.34
        tree.nodes[T('Div')].last_value, tree.nodes[T('Exp')].last_value = 10, -2      # as the platform's get records
        agent.set_point(T('V'), 100.0)
        agent.set_point(T('P'), 56.78)
        assert agent.interface.raw[T('V')] == 1000 and agent.interface.raw[T('P')] == 5678
