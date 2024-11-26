import typing
from dataclasses import dataclass


@dataclass
class TestSetting:
    entry: str
    fun: typing.Callable
    data_type: str
