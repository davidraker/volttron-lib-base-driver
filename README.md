# volttron-lib-base-driver

[![Eclipse VOLTTRON™](https://img.shields.io/badge/Eclips%20VOLTTRON--red.svg)](https://volttron.readthedocs.io/en/latest/)
![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)
![Passing?](https://github.com/eclipse-volttron/volttron-lib-base-driver/actions/workflows/run-tests.yml/badge.svg)
[![pypi version](https://img.shields.io/pypi/v/volttron-lib-base-driver.svg)](https://pypi.org/project/volttron-lib-base-driver/)


## Requirements

* python >= 3.10
* volttron-core >= 2.0.0rc0

# Documentation
More detailed information about the VOLTTRON Driver Framework can be found [ReadTheDocs](https://eclipse-volttron.readthedocs.io/en/latest/external-docs/volttron-platform-driver/index.html#platform-driver-framework). The RST source
of the documentation for this component is located in the "docs" directory of this repository.

## Installation

Before installing, VOLTTRON should be installed and running.  Its virtual environment should be active.
Information on how to install of the VOLTTRON platform can be found
[here](https://github.com/eclipse-volttron/volttron-core).

Install the library into the running VOLTTRON instance:

```shell
vctl install-lib volttron-lib-base-driver
```

To remove it later, use:

```shell
vctl remove-lib volttron-lib-base-driver
```

## Proxy-backed interfaces

Interfaces whose protocol runs in a [Protocol Proxy](https://github.com/eclipse-volttron/lib-protocol-proxy) process
(BACnet, Modbus, DNP3, IEEE 2030.5) share `volttron.driver.base.proxy_interface.ProxyBackedInterface`: the manager
wiring, the registration handshake, the reply envelope, the mapping of replies onto per-topic results and errors, the
timeout and failure handling, and the push callback into `publish_push`. A protocol interface lists it first,
`class Dnp3(ProxyBackedInterface, BasicRevert, BaseInterface)`, sets the message names as class attributes and
overrides the few hooks whose defaults do not fit (`identity_fields`, `point_fields`, `read_payload`, `read_result`,
`coerce`, `split_reads`, `split_writes`). `volttron.driver.base.testing` provides `FakePPM`, `serialized` and
`build_interface` for testing such interfaces without a proxy process. The protocol proxy is an optional dependency:
`pip install volttron-lib-base-driver[proxy]`.

The proxy manager watches the processes it launches. When an interface's proxy exits or never registers, the interface
is told, forgets the peer and runs `finalize_setup` again after a growing delay (`RECOVERY_DELAYS`: 1, 2, 5, 10, then
30 seconds between attempts): the proxy is relaunched and every remote registers with it again, without a platform
driver restart. Pushed values arriving by topic are checked twice, in `handle_pushed` (only the instance's own points)
and in `DriverAgent.publish_push` (only points of that remote), so a proxy can only affect the points it serves.

A remote configuration may name a `driver_role`. The default, `client`, reaches a device; a server role (`server`, or
the protocol's own word such as `outstation`) serves the configured points to a remote peer through the same interface
class and proxy. Each interface narrows the accepted values and validates the role-specific settings. In a server role
the registry's `Remote Writable` column says which served points the peer may write (defaulting from the protocol
address), while `Writable` keeps meaning that the platform may set the point; served rows default to the `server` data
source, so they are never polled.

## Point transforms

A registry row may carry a `Transform` column: a conversion between the raw value the protocol carries and the
engineering value the platform publishes and accepts. Reads apply it, writes apply its inverse, and the interface never
sees the engineering value, so every interface gets it for free. The functions are those of the `icm.transforms`
library (`icm-lib-transforms`), which the base driver depends on.

| CSV value | Meaning |
|---|---|
| `scale(0.001)` | one function: the modbus_tk form |
| `sunssf(W_SF)` | SunSpec scale factor held by the point `W_SF` of the same device; -32768 counts as unavailable |
| `scale_reg(Div)`, `scale_reg_pow_10(Exp)` | scaled by another point's value, or by ten to its power |
| `transform(scale(0.1), add(-40))` | a sequence, applied left to right on read and right to left on write |
| `transform(mod10k(True), cast_value(int))` | a sequence ending in a type change |
| `sum_of(A, B)`, `mean_of(A, C)`, `all_equal_of(3, A, B)` | a derived (`virtual`) point computed from other points (see below) |
| `transform(value_of(Status), bit(3))` | a derived point from one other point: here bit 3 of a status word as a point of its own, with no second register at the address |
| `writable_bit(Status, 3)` | the same bit, writable: a write sets or clears it in the word and writes the word to `Status` |
| `invalid(-32768)`, `finite()` | a device's "not implemented" or error sentinel (SunSpec: `0x8000` for int16, `0xFFFF` for uint16, NaN for float32) yields no value instead of a number; put first in a chain so it is not scaled |

A bare comma-separated list (`scale(0.1), add(-40)`) is rejected; the error shows the wrapped form. The full function
set (arithmetic, comparisons, rounding, bits, enumeration `map`, the modbus_tk functions) is the library's.

* **Configure time.** Each point's expression is compiled once. An unknown function, a bad argument, a reference to a
  point the device does not have, or a reference cycle is logged as an error and the point is configured without a
  transform, so the rest of the device still works. A writable point whose chain has no inverse is warned about;
  writes to it are refused.
* **Scale factors.** A referenced point must be in the same device's registry, named by its `Volttron Point Name`
  or, failing that, by the one row whose `Register Name` or `Reference Point Name` it is (a modbus_tk register map
  names its own registers while each device's registry chooses the published names). Its value is taken from the
  current read when it is polled in the same request, else from its last value. When it has no value anywhere the point is
  held out: no value is published and the read's errors name the missing factor, logged once until it appears.
  Referenced points that are polled are read once at start-up (with the `poll_once` points) so a last value exists
  before the first cyclic poll; a `never_poll` reference is honoured with a warning.
* **Virtual and derived points.** A row with `Data Source` `virtual` is held by the framework, never by the interface,
  and needs no address or type. Without a transform it is a value the platform sets (when `Writable`), as a BACnet value
  object is. With a transform made only of functions over other registers (`sum_of`, `mean_of`, `min_of`, `max_of`,
  `all_equal_of`, `all_of`, `any_of`, or `value_of` for a single one) it is a derived point: computed and published
  whenever a read includes any point it references, computed from last values on an RPC `get`, and not writable.
  `writable_bit(Status, 3)` is a derived bit that can be written: the write sets or clears the bit in the word's last
  value (or a fresh read with `writable_bit(Status, 3, True)`) and writes the word to `Status`. Numbers among the
  arguments are literals; a string literal is written `literal('ON')` and an all-digit point name `reg_name(3)`.
* **Metadata.** The published type follows the transform (`scale` yields a float, `equals` a boolean); units are the
  column's and are not touched. `Default Value` and revert stay in the raw domain.
* **Served roles.** The platform's writes to a served point get the inverse, the peer's writes arrive through the push
  path and get the forward transform, and the values seeded into a proxy from the equipment tree are inverted first.

## Remote health

Every `DriverAgent` keeps a `RemoteHealth` record from its own traffic: status `UNKNOWN` until the first poll, `GOOD`
while polls return values, `BAD` once a poll returns nothing or raises; poll and write counters, consecutive failures,
the last success and failure times, the last error, the points that failed in the latest poll with their messages,
and recent events (a proxy process lost or recovered). An interface may add what its protocol knows by defining
`health()` returning a dict (the Modbus driver returns the proxy's connection counters, the write functions the unit
refused and the device identification). `DriverAgent.health_report()` returns both; the platform driver exposes it as
the `health` RPC and `vctl driver health`, and sets `health.on_change` to be told when a remote changes status.

## Registry rows that cannot be configured

A row that fails validation, or whose register the interface cannot create, is logged with the point name and dropped;
the rest of the device configures. Registry columns the interface does not define are listed once per device in a
warning, with the interface's hint for legacy columns where it has one.

## Development

Please see the following for contributing guidelines [contributing](https://github.com/eclipse-volttron/volttron-core/blob/develop/CONTRIBUTING.md).

Please see the following helpful guide about [developing modular VOLTTRON agents](https://github.com/eclipse-volttron/volttron-core/blob/develop/DEVELOPING_ON_MODULAR.md)


## Disclaimer Notice

This material was prepared as an account of work sponsored by an agency of the
United States Government.  Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in the development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or any
information, apparatus, product, software, or process disclosed, or represents
that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the United
States Government or any agency thereof, or Battelle Memorial Institute. The
views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.
