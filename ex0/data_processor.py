# ************************************************************************* #
#                                                                           #
#                                                      :::      ::::::::    #
#  data_processor.py                                 :+:      :+:    :+:    #
#                                                  +:+ +:+         +:+      #
#  By: ksener <ksener@student.42kocaeli.com.tr   +#+  +:+       +#+         #
#                                              +#+#+#+#+#+   +#+            #
#  Created: 2026/06/17 12:56:31 by ksener          #+#    #+#               #
#  Updated: 2026/06/17 12:56:31 by ksener          ###   ########.fr        #
#                                                                           #
# ************************************************************************* #


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


if __name__ == "__main__":
    print("=== Code Nexus Data Processor ===\n")

    print("Testing Numeric Processor...")
    num_proc = NumericProcessor()

    test: typing.Any = 42
    print(f"Trying to validate input '{test}': {num_proc.validate(test)}")
    test = "Hello"
    print(f"Trying to validate input '{test}': {num_proc.validate(test)}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_proc.ingest("foo")
    except Exception as e:
        print(f"Got exception: {e}")

    numeric_data: list[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {numeric_data}")
    num_proc.ingest(numeric_data)

    print("Extracting 3 values...")
    for _ in 0, 1, 2:
        rank, val = num_proc.output()
        print(f"Numeric value {rank}:", end=" ")
        print(val)

    print("\nTesting Text Processor...")
    text_proc = TextProcessor()

    test = 61
    print(f"Trying to validate input '{test}': {text_proc.validate(test)}")

    text_data = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {text_data}")
    text_proc.ingest(text_data)

    print("Extracting 1 value...")
    t_rank, t_val = text_proc.output()
    print(f"Text value {t_rank}: {t_val}\n")

    print("Testing Log Processor...")
    log_proc = LogProcessor()

    test = "Hello"
    print(f"Trying to validate input {test}: {log_proc.validate(test)}")

    log_data = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f"Processing data: {log_data}")
    log_proc.ingest(log_data)

    print("Extracting 2 values...")
    for _ in 0, 1:
        l_rank, l_val = log_proc.output()
        print(f"Log entry {l_rank}: {l_val}")
