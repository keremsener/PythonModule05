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


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVExporter:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        for idx, item in enumerate(data):
            if idx > 0:
                print(",", end="")
            print(item[1], end="")
        print()


class JSONExporter:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        print("{", end="")
        for idx, item in enumerate(data):
            if idx > 0:
                print(", ", end="")
            print(f'"item_{item[0]}": "{item[1]}"', end="")
        print("}")


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
                print(f"DataStream error -\n Can't process element in stream: {item}")

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")
        
        if not self.processors:
            print("No processor found, no data")
            return

        for proc in self.processors:
            cls_name = proc.__class__.__name__
            proc_name = cls_name.replace("Processor", " Processor")
            print(f"{proc_name}: total {proc.counter} items processed, "
                  f"remaining {len(proc.storage)} on processor")
    
    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for item in self.processors:
            output_list = []
            i = 0
            while i != nb:
                try:
                    output_list.append(item.output())
                except IndexError:
                    break            
                i += 1
            if output_list:
                plugin.process_output(output_list)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")
    print("Initialize Data Stream...\n")
    
    stream_manager = DataStream()
    stream_manager.print_processors_stats()
    
    print("\nRegistering Processors\n")
    num_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    
    stream_manager.register_processor(num_proc)
    stream_manager.register_processor(text_proc)
    stream_manager.register_processor(log_proc)

    batch1 = [
        'Hello world', 
        [3.14, -1, 2.71], 
        [
            {'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'}, 
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ], 
        42, 
        ['Hi', 'five']
    ]
    
    print(f"Send first batch of data on stream: {batch1}")
    stream_manager.process_stream(batch1)
    stream_manager.print_processors_stats()

    csv_plugin = CSVExporter()
    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream_manager.output_pipeline(3, csv_plugin)
    stream_manager.print_processors_stats()

    batch2 = [
        21, 
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'], 
        [
            {'log_level': 'ERROR', 'log_message': '500 server crash'}, 
            {'log_level': 'NOTICE', 'log_message': 'Certificate expires in 10 days'}
        ],
        [32, 42, 64, 84, 128, 168], 
        'World hello'
    ]
    
    print(f"Send another batch of data: {batch2}")
    stream_manager.process_stream(batch2)
    stream_manager.print_processors_stats()

    json_plugin = JSONExporter()
    print("Send 5 processed data from each processor to a JSON plugin:")
    stream_manager.output_pipeline(5, json_plugin)
    stream_manager.print_processors_stats()