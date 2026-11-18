"""STATIC points: never polled, published once when configured and whenever they are written (B10 of the poll
scheduler plan)."""
from unittest import mock

import pytest

from test_transforms import DEVICE, T, fake_module, publishes, row, world  # noqa: F401  (fixtures)

pytest.importorskip('icm.transforms')


class TestStaticPublication:
    def test_publish_static_uses_the_last_value_or_the_default(self, world):
        agent, tree = world
        agent.build([row('Mode', data_source='static', type='int', writable=True), row('Live', type='int')])
        mode = tree.nodes[T('Mode')]
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            agent.publish_static(tree.device_points(DEVICE))
        assert publishes(publish) == {}                                  # no last value and no Default Value: nothing
        mode.last_value = 3
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            agent.publish_static(tree.device_points(DEVICE))
        out = publishes(publish)
        assert out[T('Mode')][0] == 3 and out[f'{DEVICE}/multi'][0] == {'Mode': 3}

    def test_a_write_to_a_static_point_publishes_it_without_transforming_again(self, world):
        agent, tree = world
        agent.build([row('Setpoint', 'scale(0.1)', data_source='static', type='int', writable=True), row('Live', type='int', writable=True)])
        with mock.patch('volttron.driver.base.driver.publish_wrapper') as publish:
            results, errors = agent.set_multiple_points([(T('Setpoint'), 21.5), (T('Live'), 7)])
        assert errors == {} and results[T('Setpoint')] == 21.5
        out = publishes(publish)
        assert out[T('Setpoint')][0] == 21.5 and out[f'{DEVICE}/multi'][0] == {'Setpoint': 21.5}   # engineering value, once
        assert tree.nodes[T('Setpoint')].last_value == 21.5 and T('Live') not in out
        assert agent.interface.raw[T('Setpoint')] == 215                     # the device got the raw value
