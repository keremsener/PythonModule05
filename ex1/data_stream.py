import abc
import typing

LogDict = dict[str, str]


class DataProcessor(abc.ABC):  # Ana classımız
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
        flag = False
        if type(data) in (int, float):
            return True
        if type(data) is list:
            if len(data) == 0:
                return False
            for item in data:
                if type(item) in (int, float):
                    flag = True
                else:
                    flag = False
                    return flag
            return flag
        return flag

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if type(data) is list:
            list_data = typing.cast(list[int | float], data)
            for item in list_data:
                self.storage.append((self.counter, str(item)))
                self.counter += 1
        else:
            self.storage.append((self.counter, str(data)))
            self.counter += 1


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        flag = False
        if type(data) is str:
            return True
        if type(data) is list:
            if len(data) == 0:
                return False
            for item in data:
                if type(item) is str:
                    flag = True
                else:
                    flag = False
                    return flag
            return flag
        return flag

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
        flag = False
        if type(data) is dict:
            if "log_level" in data and "log_message" in data:
                if (type(data["log_level"]) is str and
                        type(data["log_message"]) is str):
                    return True
        if type(data) is list:
            if len(data) == 0:
                return False
            for item in data:
                if type(item) is not dict:
                    return flag
                if "log_level" not in item or "log_message" not in item:
                    return flag

                if type(item["log_level"]) is not str or type(
                        item["log_message"]) is not str:
                    return flag
            flag = True
            return flag
        return flag

    def ingest(self, data: LogDict | list[LogDict]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if type(data) is list:
            list_data = typing.cast(list[LogDict], data)

            for item in list_data:
                lvl = item['log_level']
                msg = item['log_message']
                formatted_log = f"{lvl}: {msg}"
                self.storage.append(
                    (self.counter, formatted_log))
                self.counter += 1

        else:
            dict_data = typing.cast(LogDict, data)
            lvl = dict_data['log_level']
            msg = dict_data['log_message']
            formatted_log = f"{lvl}: {msg}"
            self.storage.append((self.counter, formatted_log))
            self.counter += 1


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            processed = False

            for proc in self.processors:
                if proc.validate(item):
                    proc.ingest(item)
                    processed = True
                    break

            if not processed:
                print(f"DataStream error -\
 Can't process element in stream: {item}")

    def print_processors_stats(self) -> None:
        if not self.processors:
            print("No processor found, no data")
            return

        for proc in self.processors:
            cls_name = proc.__class__.__name__
            proc_name = cls_name.replace("Processor", " Processor")
            print(f"{proc_name}: total {proc.counter} items processed,\
 remaining {len(proc.storage)} on processor")
