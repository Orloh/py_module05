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

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """Abstract base class representing a generic data stream."""
    def __init__(self, stream_id: str) -> None:
        self.stream_id: str = stream_id
        self.stream_type: str = "generic"
        self.stream_stats: Dict[str, Union[str, int, float]] = {}

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data. Must be overriden by subclasses."""
        pass

    def filter_data(
            self,
            data_batch: List[Any],
            criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter data based on criteria. Can be overridden if needed."""
        if not criteria:
            return data_batch
        return [
            item for item in data_batch
            if criteria.lower() in str(item).lower()
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics bundled with idenrtifying metadata."""
        payload: Dict[str, Union[str, int, float]] = {
            "id": (self.stream_id),
            "type": (self.stream_type)
        }
        payload.update(self.stream_stats)
        return payload


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "sensor"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "high_priority":
            critical_items = []
            for item in data_batch:
                if ":" in str(item):
                    val = float(str(item).split(":")[1].strip())
                    if val > 40.0:
                        critical_items.append(item)
            return critical_items

        return super().filter_data(data_batch, criteria)

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            parsed_readings = [
                (
                    str(item).split(":")[0].strip().lower(),
                    float(str(item).split(":")[1].strip())
                ) for item in data_batch if ":" in str(item)
            ]

            grouped_data: Dict[str, List[float]] = {}
            for metric, value in parsed_readings:
                if metric not in grouped_data:
                    grouped_data[metric] = []
                grouped_data[metric].append(value)

            self.stream_stats["total_items"] = len(data_batch)
            for metric, values in grouped_data.items():
                avg = sum(values) / len(values) if values else 0.0
                self.stream_stats[f"avg_{metric}"] = round(avg, 2)

            return f"[{', '.join(data_batch)}]"

        except Exception as error:
            return f"Error in processing data batch: {error}"


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "transaction"

    def filter_data(
        self,
        data_batch: List,
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Overridden filter designed to handle dictionaries"""
        if not criteria:
            return data_batch

        elif criteria == "high_priority":
            return [
                item for item in data_batch
                if isinstance(item, dict) and
                [val for val in item.values() if val >= 1000]
            ]

        return [
            item for item in data_batch
            if isinstance(item, dict) and
            criteria.lower() in [str(k).lower() for k in item.keys()]
        ]

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            net_flow = 0
            total_ops = len(data_batch)
            processed_stream = []

            for item in data_batch:
                if isinstance(item, dict):
                    for action, value in item.items():
                        action_str = str(action).lower().strip()

                        if "buy" in action_str:
                            net_flow += value
                        elif "sell" in action_str:
                            net_flow -= value

                        processed_stream.append(f"{action_str}:{value}")
            self.stream_stats["total_items"] = total_ops
            self.stream_stats["net_flow"] = net_flow

            return f"[{', '.join(processed_stream)}]"

        except Exception as error:
            return f"Error in processing transaction batch: {error}"


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "event"

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            errors = [
                item for item in data_batch
                if "error" in str(item).lower()
            ]

            self.stream_stats["total_items"] = len(data_batch)
            self.stream_stats["error_count"] = len(errors)

            return f"{data_batch}"
        except Exception as error:
            return f"Error in processing event batch: {error}"


class StreamProcessor:
    """Manager class that handles multiple stream types polymorphically."""
    def __init__(self) -> None:
        self.active_streams: Dict[str, DataStream] = {}

    def register_stream(self, stream: DataStream) -> None:
        """Registers a stream into the Nexus using its unique ID."""
        if isinstance(stream, DataStream):
            self.active_streams[stream.stream_id] = stream
        else:
            print("Error: Invalid stream. Must inherit from DataStream.")

    def process_stream(self, stream_id: str, data_batch: List[Any]) -> str:
        """
        Processes a single batch for a specific stream.
        Returns the processed string or an error message.
        """
        if stream_id in self.active_streams:
            stream = self.active_streams[stream_id]
            return stream.process_batch(data_batch)
        else:
            return f"Error: Stream '{stream_id}' not found in the Nexus."

    def process_all_streams(
            self,
            data_payloads: Dict[str, List[Any]]
    ) -> List[str]:
        """
        Uses the targeted method to process multiple batches.
        """
        return [
            self.process_stream(stream_id, batch)
            for stream_id, batch in data_payloads.items()
        ]

    def generate_report(self) -> None:
        """
        Polls all registered streams for their statistics and
        prints a unified Nexus report.
        """
        for stream_id, stream in self.active_streams.items():
            stats = stream.get_stats()
            stream_type = str(stats.get("type", "unknown"))
            total = stats.get("total_items", 0)
            if stream_type == "sensor":
                noun = "readings"
            elif stream_type == "transaction":
                noun = "operations"
            elif stream_type == "event":
                noun = "events"
            else:
                noun = "items"

            total_str = f"{total} {noun} processed"
            print(f"- {stream_type.capitalize()} data: {total_str}")
        print()

    def run_filter_diagnostics(
            self,
            data_payloads: Dict[str, List[Any]],
            mode: str = "normal"
    ) -> None:
        """
        Applies criteria filtering across all streams polymorphically.
        """
        if mode == "high_priority":
            print("Stream filtering active: High-priority data only")
        else:
            print("Stream filtering active: Normal processing mode")
            return

        sensor_alerts = 0
        large_tx = 0

        for stream_id, batch in data_payloads.items():
            if stream_id in self.active_streams:
                stream = self.active_streams[stream_id]
                filtered_batch = stream.filter_data(batch, criteria=mode)

                if stream.stream_type == "sensor":
                    sensor_alerts += len(filtered_batch)
                elif stream.stream_type == "transaction":
                    large_tx += len(filtered_batch)

        alert_word = "alert" if sensor_alerts == 1 else "alerts"
        sensor_str = f"{sensor_alerts} critical sensor {alert_word}"

        tx_word = "transaction" if large_tx == 1 else "transactions"
        tx_str = f"{large_tx} large {tx_word}"

        print(f"Filtered results: {sensor_str}, {tx_str}")


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    sensor_batch = ["temp:22.5", "humidity:65", "pressure: 1013"]
    print(f"Processing sensor batch: {sensor.process_batch(sensor_batch)}")
    sensor_stats = sensor.get_stats()
    readings = sensor_stats.get("total_items", 0)
    readings_str = f"{readings} readings processed"
    avg_temp = sensor_stats.get("avg_temp", 0.0)
    temp_str = f"avg temp: {avg_temp}ºC"
    print(f"Sensor analysis: {readings_str}, {temp_str}")
    print()

    print("Initializing Transaction Stream...")
    tx = TransactionStream("TRANS_001")
    tx_batch = [{"buy": 100}, {"sell": 150}, {"buy": 75}]
    print(f"Processing transaction batch: {tx.process_batch(tx_batch)}")
    tx_stats = tx.get_stats()
    ops = tx_stats.get("total_items", 0)
    ops_str = f"{ops} operations processed"
    net_flow = tx_stats.get("net_flow", 0)
    net_flow_str = f"net flow: {net_flow:+d}"
    print(f"Transaction analysis: {ops_str}, {net_flow_str}")
    print()

    print("Initializing Event Stream...")
    event = EventStream("EVENT_001")
    event_batch = ["login", "error", "logout"]
    print(f"Processing event batch: {event.process_batch(event_batch)}")
    event_stats = event.get_stats()
    total_events = event_stats.get("total_items", 0)
    total_events_str = f"{total_events} events processed"
    error_count = event_stats.get("error_count", 0)
    error_count_str = f"{error_count} error detected"
    print(f"Event analysis: {total_events_str} , {error_count_str}")
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print()

    processor = StreamProcessor()
    streams = [sensor, tx, event]
    for stream in streams:
        processor.register_stream(stream)

    mixed_payloads: Dict[str, List[Any]] = {
        "EVENT_001": ["system_start", "error", "login", "error"],
        "SENSOR_001": [
            "temp: 18.0",
            "temp:41.0",
            "humidity:40",
            "uv_index: 2"
        ],
        "TRANS_001": [{"buy": 1500}, {"sell": 1200}, {"sell": 400}]
    }

    print("Batch 1 Results:")
    processor.process_all_streams(mixed_payloads)
    processor.generate_report()
    processor.run_filter_diagnostics(mixed_payloads, mode="high_priority")
    print()

    print("All streams processed succesfully. Nexus throughput optimal")


if __name__ == "__main__":
    main()
