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
            list_data = typing.cast(list[str], data)
            for item in list_data:
                self.storage.append((self.counter, item))
                self.counter += 1
        else:
            str_data = typing.cast(str, data)
            self.storage.append((self.counter, str_data))
            self.counter += 1


class LogProcessor(DataProcessor):

    def validate(self, data: typing.Any) -> bool:
        if type(data) is dict:
            if "log_level" in data and "log_message" in data:
                if (type(data["log_level"]) is str and
                        type(data["log_message"]) is str):
                    return True
            return False

        if type(data) is list:
            if len(data) == 0:
                return False
            return all(
                type(item) is dict and
                "log_level" in item and
                "log_message" in item and
                type(item["log_level"]) is str and
                type(item["item_message"]) is str
                for item in data
            )
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if type(data) is list:
            list_data = typing.cast(list[dict[str, str]], data)

            for item in list_data:
                lvl = item['log_level']
                msg = item['log_message']
                formatted_log = f"{lvl}: {msg}"
                self.storage.append((self.counter, formatted_log))
                self.counter += 1

        else:
            dict_data = typing.cast(dict[str, str], data)
            lvl = dict_data['log_level']
            msg = dict_data['log_message']
            formatted_log = f"{lvl}: {msg}"
            self.storage.append((self.counter, formatted_log))
            self.counter += 1
