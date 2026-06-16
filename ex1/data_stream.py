import abc
import typing

LogDict = dict[str, str]


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


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print("Initialize Data Stream...")

    stream_manager = DataStream()

    print("== DataStream statistics ==")
    stream_manager.print_processors_stats()

    print("Registering Numeric Processor")
    num_proc = NumericProcessor()
    stream_manager.register_processor(num_proc)
    text = "Telnet access! Use ssh instead"
    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING', 'log_message': text},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ],
        42,
        ['Hi', 'five']
    ]

    print(f"Send first batch of data on stream: {batch}")
    stream_manager.process_stream(batch)

    print("== DataStream statistics ==")
    stream_manager.print_processors_stats()

    print("Registering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream_manager.register_processor(text_proc)
    stream_manager.register_processor(log_proc)

    print("Send the same batch again")
    stream_manager.process_stream(batch)

    print("== DataStream statistics ==")
    stream_manager.print_processors_stats()

    print("Consume some elements from the data processors:\
 Numeric 3, Text 2, Log 1")
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        text_proc.output()
    for _ in range(1):
        log_proc.output()

    print("== DataStream statistics ==")
    stream_manager.print_processors_stats()
