import abc
import typing

class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.storage: list[tuple[int, str]] = []
        self.counter: int = 0
    
    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.storage:
            raise IndexError("Processor storage is empty.")
        
        return self.storage.pop(0)

class NumericProcessor(DataProcessor):

    def validate(self, data: typing.Any) -> bool:
        if type(data) in (int, float):
            return True

        if type(data) is list:
            if len(data) == 0:
                return False
            return all(type(item) in (int, float) for item in data)
        return False
    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if type(data) is list:
            for item in data:
                self.storage.append((self.counter, str(item)))
                self.counter += 1
        else:
            self.storage.append((self.counter, str(data)))
            self.counter += 1

class TextProcessor(DataProcessor):

    def validate(self, data: typing.Any) -> bool:
        if type(data) is str:
            return True

        if type(data) is list:
            if len(data) == 0:
                return False
            return all(type(item) is str for item in data)
            
        return False
    
    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
            
        if type(data) is list:
            for item in data:
                self.storage.append((self.counter, item))
                self.counter += 1
        else:
            self.storage.append((self.counter, data))
            self.counter += 1