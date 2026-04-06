# *************************************************************************** #
#                                                                             #
#                                                        :::      ::::::::    #
#    data_pipeline.py                                  :+:      :+:    :+:    #
#                                                    +:+ +:+         +:+      #
#    By: orhernan <ohercelli@gmail.com>            +#+  +:+       +#+         #
#                                                +#+#+#+#+#+   +#+            #
#    Created: 2026/04/06 21:36:00 by orhernan         #+#    #+#              #
#    Updated: 2026/04/06 21:36:59 by orhernan        ###   ########.fr        #
#                                                                             #
# *************************************************************************** #

import abc
from typing import Any, Protocol


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        values = [val for rank, val in data]
        csv_string = ",".join(values)
        print(csv_string)


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        items = [f'"item_{rank}": "{val}"' for rank, val in data]
        json_string = "{" + ",".join(items) + "}"
        print(json_string)


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

    def register_processor(self, proc: DataProcessor) -> None:
        self.__processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            handled = False
            for proc in self.__processors:
                if proc.validate(item):
                    proc.ingest(item)
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

        for proc in self.__processors:
            display_name = f"{proc._type.capitalize()} Processor"

            total = proc._rank_counter
            remaining = len(proc._storage)
            print(
                f"{display_name}: total {total} items processed, "
                f"remaining {remaining} on processor"
            )

        print()

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.__processors:
            export_data: list[tuple[int, str]] = []

            for _ in range(nb):
                try:
                    export_data.append(proc.output())
                except IndexError:
                    break

            if export_data:
                plugin.process_output(export_data)


def main() -> None:
    print("=== Code Nexus Data Pipeline ===")
    print()

    print("Initialize Data Stream...")
    stream = DataStream()
    print()

    stream.print_processors_stats()

    print("Registering Processors...")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())
    print()

    batch1 = [
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

    print(f"Send first batch of data on stream: {batch1}")
    stream.process_stream(batch1)
    print()

    stream.print_processors_stats()

    print("Send 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    stream.output_pipeline(3, csv_plugin)
    print()

    stream.print_processors_stats()

    batch2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [
            {
                'log_level': 'ERROR',
                'log_message': '500 server crash'
            },
            {
                'log_level': 'NOTICE',
                'log_message': 'Certificate expires in 10 days'
            }
        ],
        [32, 42, 64, 84, 128, 168],
        'World Hello'
    ]

    print(f"Send another batch of data: {batch2}")
    stream.process_stream(batch2)
    print()

    stream.print_processors_stats()

    print("Send 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    stream.output_pipeline(5, json_plugin)
    print()

    stream.print_processors_stats()


if __name__ == "__main__":
    main()
