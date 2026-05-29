import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        pass
