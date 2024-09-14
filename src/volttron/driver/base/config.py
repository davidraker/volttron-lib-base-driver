from enum import StrEnum
from pydantic import BaseModel, computed_field, ConfigDict, Field, field_validator

# TODO: Wire up the data_source field to poll scheduling (everything is currently short-poll because this isn't used).
# TODO: Should NEVER actually be an option? Could it just be None?
DataSource = StrEnum('DataSource', ['SHORT_POLL', 'LONG_POLL', 'NEVER', 'POLL_ONCE', 'STATIC'])


class EquipmentConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True)
    active: bool = True
    group: int | None = None
    meta_data: dict = {}
    polling_interval: float | None = Field(default=None, alias='interval')
    publish_single_depth: bool | None = Field(default=None, alias='publish_depth_first_single')
    publish_single_breadth: bool | None = Field(default=None, alias='publish_breadth_first_single')
    publish_multi_depth: bool | None = Field(default=None, alias='publish_depth_first_multi')
    publish_multi_breadth: bool | None = Field(default=None, alias='publish_breadth_first_multi')
    publish_all_depth: bool | None = Field(default=None, alias='publish_depth_first_all')
    publish_all_breadth: bool | None = Field(default=None, alias='publish_breadth_first_all')
    reservation_required_for_write: bool = False

class PointConfig(EquipmentConfig):
    data_source: DataSource = Field(default='short_poll', alias='Data Source')
    reference_point_name: str = Field(default='', alias='Reference Point Name')
    volttron_point_name: str = Field(alias='Volttron Point Name')
    units: str = Field(default='', alias='Units')
    units_details: str = Field(default='', alias='Unit Details')
    configured_stale_timeout: float | None = Field(default=None, alias='stale_timeout')
    stale_timeout_multiplier: float = Field(default=3)
    writable: bool = Field(default=False, alias='Writable')
    notes: str = Field(default='', alias='Notes')

    # noinspection PyMethodParameters
    @field_validator('data_source', mode='before')
    def _normalize_data_source(cls, v):
        return v.lower()

    @computed_field
    @property
    def stale_timeout(self) -> float | None:
        if self.configured_stale_timeout is None and self.polling_interval is None:
            return None
        else:
            return (self.configured_stale_timeout
                    if self.configured_stale_timeout is not None
                    else self.polling_interval * self.stale_timeout_multiplier)

    @stale_timeout.setter
    def stale_timeout(self, value):
        self.configured_stale_timeout = value


class DeviceConfig(EquipmentConfig):
    all_publish_interval: float = 0.0
    allow_duplicate_remotes: bool = False
    equipment_specific_fields: dict = {}
    registry_config: list[PointConfig] = []


class RemoteConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    driver_type: str
    heart_beat_point: str | None = None
    module: str | None = None