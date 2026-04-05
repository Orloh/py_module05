# *************************************************************************** #
#                                                                             #
#                                                        :::      ::::::::    #
#    stream_processor.py                               :+:      :+:    :+:    #
#                                                    +:+ +:+         +:+      #
#    By: orhernan <ohercelli@gmail.com>            +#+  +:+       +#+         #
#                                                +#+#+#+#+#+   +#+            #
#    Created: 2026/03/30 19:31:53 by orhernan         #+#    #+#              #
#    Updated: 2026/03/30 21:19:40 by orhernan        ###   ########.fr        #
#                                                                             #
# *************************************************************************** #

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataProcessor(ABC):
    """
    Abstract base class defining the common processing interface.
    """
    def __init__(self) -> None:
        self.initialized = True

    @abstractmethod
    def process(self, data: Any) -> str:
        """
        Process the data and return result.
        """
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """
        Validate if data is appropriate for this processor.
        """
        pass

    def format_output(self, result: str) -> str:
        """
        Format the output string.
        """
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False
        for number in data:
            if not isinstance(number, (int, float)):
                return False
        return True

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid numeric data stream")

            total = sum(data)
            total_str = f"sum={total}"

            count = len(data)
            count_str = f"{count} numeric values"

            avg = total / count if data else 0
            avg_str = f"avg={avg:.1f}"

            return f"Processed {count_str}, {total_str}, {avg_str}"
        except Exception as error:
            return f"Error: {error}"


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid text data stream")
            chars = len(data)
            words = len(data.split())
            return f"Processed text: {chars} characters, {words} words"
        except Exception as error:
            return f"Error: {error}"


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if not isinstance(data, dict) or len(data) != 1:
            return False
        [(key, value)] = data.items()
        return isinstance(key, str) and isinstance(value, str)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid log format")
            [(level, message)] = data.items()
            prefix = "[ALERT]" if level == "ERROR" else f"[{level}]"
            return f"{prefix} {level} level detected: {message}"
        except Exception as error:
            return f"Error: {error}"


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    print("Initializing Numeric Processor...")
    num_proc = NumericProcessor()
    num_data = [1, 2, 3, 4, 5]
    print(f"Processing data: {num_data}")
    if num_proc.validate(num_data):
        print("Validation: Numeric data verified")
        print(num_proc.format_output(num_proc.process(num_data)))
    print()

    print("Initializing Text Processor...")
    txt_proc = TextProcessor()
    txt_data = "Hello Nexus World"
    print(f"Processing data: '{txt_data}'")
    if txt_proc.validate(txt_data):
        print("Validation: Text data verified")
        print(txt_proc.format_output(txt_proc.process(txt_data)))
    print()

    print("Initializing Log Processor...")
    log_proc = LogProcessor()
    log_data = {"ERROR": "Connection timeout"}
    print(f"Processing data: '{log_data}'")
    if log_proc.validate(log_data):
        print("Validation: Log entry verified")
        print(log_proc.format_output(log_proc.process(log_data)))
    print()

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    processors: List[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor()
    ]

    mixed_data: List[Any] = [
        [1, 2, 3],
        "Nexus Online",
        {"INFO": "System ready"}
    ]

    for i in range(len(processors)):
        proc = processors[i]
        data = mixed_data[i]
        if proc.validate(data):
            print(f"Result {i + 1}: {proc.process(data)}")

    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
