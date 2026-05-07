#!/usr/bin/env python3
# *************************************************************************** #
#                                                                             #
#                                                        :::      ::::::::    #
#    data_stream.py                                    :+:      :+:    :+:    #
#                                                    +:+ +:+         +:+      #
#    By: orhernan <ohercelli@gmail.com>            +#+  +:+       +#+         #
#                                                +#+#+#+#+#+   +#+            #
#    Created: 2026/03/31 19:17:27 by orhernan         #+#    #+#              #
#    Updated: 2026/04/04 01:36:11 by orhernan        ###   ########.fr        #
#                                                                             #
# *************************************************************************** #

import abc
from typing import Any


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self._rank_counter: int = 0
        self._type = "generic"

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
    def __init__(self) -> None:
        super().__init__()
        self._type = "numeric"

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
    def __init__(self) -> None:
        super().__init__()
        self._type = "text"

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
    def __init__(self) -> None:
        super().__init__()
        self._type = "log"

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


class DataStream:
    def __init__(self) -> None:
        self.__processors: list[DataProcessor] = []

    def register_processor(self, processor: DataProcessor) -> None:
        self.__processors.append(processor)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            handled = False
            for processor in self.__processors:
                if processor.validate(item):
                    processor.ingest(item)
                    handled = True
                    break

            if not handled:
                print(
                    f"DataStream error - "
                    f"Can't process element in stream: {item}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if not self.__processors:
            print("No processor found, no data")
            print()
            return

        for processor in self.__processors:
            display_name = f"{processor._type.capitalize()} Processor"

            total = processor._rank_counter
            remaining = len(processor._storage)
            print(
                f"{display_name}: total {total} items processed, "
                f"remaining {remaining} on processor"
            )

        print()


def main() -> None:
    print("=== Code Nexus - Data Stream ===")

    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("Registering Numeric Processor")
    num_processor = NumericProcessor()
    stream.register_processor(num_processor)
    print()

    batch = [
        'Hello world',
        [3.14, 1, 2.71],
        [
            {
                'log_level': 'WARNING',
                'log_message': 'Telnet access! Use ssh instead'
            },
            {
                'log_level': 'INFO',
                'log_message': 'User wil is connected'
            }
        ],
        42,
        ['Hi', 'five']
    ]

    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    print()

    stream.print_processors_stats()

    print("Registering other data processors")
    text_processor = TextProcessor()
    log_processor = LogProcessor()
    stream.register_processor(text_processor)
    stream.register_processor(log_processor)
    print()

    print("Send the same batch again")
    stream.process_stream(batch)
    print()

    stream.print_processors_stats()

    print(
        "Consume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )

    for _ in range(3):
        num_processor.output()

    for _ in range(2):
        text_processor.output()

    for _ in range(1):
        log_processor.output()

    print()

    stream.print_processors_stats()


if __name__ == "__main__":
    main()
