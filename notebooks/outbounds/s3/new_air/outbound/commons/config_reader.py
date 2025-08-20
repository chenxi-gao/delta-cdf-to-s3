from pydantic.v1 import BaseModel, validator
from .destination import DestinationType
from typing import List, Union, IO, Optional
import yaml


class TableConfig(BaseModel):
    source_table: str
    target_table: Optional[str] = None
    merge_keys: List[str]

    @validator('target_table', pre=True, always=True)
    def set_target_table(cls, v, values):
        return v or values['source_table']


class GroupConfig(BaseModel):
    group_name: str
    max_retries: Optional[int] = 1
    alert_emails: Optional[List[str]] = None
    alert_on_failure: Optional[bool] = False
    rollback_on_error: Optional[bool] = False
    create_table_auto: Optional[bool] = True
    cdf_history_query_partition: Optional[int] = 200
    tables: List[TableConfig]

    # Validation to ensure boolean fields are always Boolean type
    @validator('alert_on_failure', 'rollback_on_error', pre=True, always=True)
    def ensure_boolean(cls, v):
        if isinstance(v, str):
            return v.lower() == 'true'
        return bool(v)


class Settings(BaseModel):
    layer: str
    target_schema: Optional[str] = None
    groups: List[GroupConfig]
    destination_type: str

    @validator('target_schema', pre=True, always=True)
    def set_target_schema(cls, v, values):
        return v or values['layer']

    # Validation to ensure at least one group is defined
    @validator('groups')
    def validate_groups(cls, v):
        if not v or len(v) == 0:
            raise ValueError('At least one group must be present in settings.')
        return v

    @validator('destination_type')
    def validate_destination_type(cls, v):
        if v not in [dt.value for dt in DestinationType]:
            allowed_values = ', '.join([dt.value for dt in DestinationType])
            raise ValueError(f"{v} is not a valid destination type. Allowed values are: {allowed_values}")
        return v


class Config(BaseModel):
    settings: Settings

    @classmethod
    def from_yaml(cls, src: Union[str, IO[str]]):
        content = yaml.safe_load(src)
        return cls(**content)
