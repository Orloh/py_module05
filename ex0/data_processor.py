# *************************************************************************** #
#                                                                             #
#                                                        :::      ::::::::    #
#    data_processor.py                                 :+:      :+:    :+:    #
#                                                    +:+ +:+         +:+      #
#    By: orhernan <ohercelli@gmail.com>            +#+  +:+       +#+         #
#                                                +#+#+#+#+#+   +#+            #
#    Created: 2026/04/06 17:01:57 by orhernan         #+#    #+#              #
#    Updated: 2026/04/06 20:07:22 by orhernan        ###   ########.fr        #
#                                                                             #
# *************************************************************************** #

import abc
from typing import Any


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self._rank_counter: int = 0

    @abc.abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("No data available to output.")

        return self._storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True
        if isinstance(data, list):
            return all(
                isinstance(item, (int, float)) and not isinstance(item, bool)
                for item in data
            )
        return False

    def ingest(self, data: int | float | list[int] | list[float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        items = data if isinstance(data, list) else [data]

        for item in items:
            self._storage.append((self._rank_counter, str(item)))
            self._rank_counter += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        items = data if isinstance(data, list) else [data]

        for item in items:
            self._storage.append((self._rank_counter, str(item)))
            self._rank_counter += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def is_valid_log(d: Any) -> bool:
            return isinstance(d, dict) and all(
                isinstance(k, str) and isinstance(v, str) for k, v in d.items()
            )

        if is_valid_log(data):
            return True
        if isinstance(data, list):
            return all(is_valid_log(item) for item in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        items = data if isinstance(data, list) else [data]

        for item in items:
            level_str = f"{item.get('log_level', 'UNKNOWN')}"
            message_str = f"{item.get('log_message', '')}"
            formatted_str = f"{level_str}: {message_str}"
            self._storage.append((self._rank_counter, formatted_str))
            self._rank_counter += 1


def main() -> None:
    print("=== Code Nexus - Data Processor ===")

    print("Testing Numeric Processor...")
    num_proc = NumericProcessor()

    print(f"\tTrying to validate input '42': {num_proc.validate(42)}")
    print(f"\tTrying to validate input 'Hello': {num_proc.validate('Hello')}")

    print("\tTest invalid ingestion of string 'foo' without prior validation:")
    try:
        num_proc.ingest("foo")
    except Exception as e:
        print(f"\tGot exception: {e}")

    num_data = [1, 2, 3, 4, 5]
    print(f"\tProcessing data: {num_data}")
    num_proc.ingest(num_data)

    print("\tExtracting 3 values...")
    for _ in range(3):
        rank, val = num_proc.output()
        print(f"\tNumeric value {rank}: {val}")

    print()

    print("Testing Text Processor...")
    text_proc = TextProcessor()

    print(f"\tTrying to validate input '42': {text_proc.validate(42)}")
    text_data = ['Hello', 'Nexus', 'World']
    print(f"\tProcessing data: {text_data}")
    text_proc.ingest(text_data)

    print("\tExtracting 1 value...")
    rank, val = text_proc.output()
    print(f"\tText value {rank}: {val}")

    print()
    print("Testing Log Processor...")
    log_proc = LogProcessor()

    print(f"\tTrying to validate input 'Hello': {log_proc.validate('Hello')}")
    log_data = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!!'}
    ]
    print(f"\tProcessing data: {log_data}")
    log_proc.ingest(log_data)

    print("\tExtracting 2 values...")
    for _ in range(2):
        rank, val = log_proc.output()
        print(f"\tLog entry {rank}: {val}")


if __name__ == "__main__":
    main()
