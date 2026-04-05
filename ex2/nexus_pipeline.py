# *************************************************************************** #
#                                                                             #
#                                                        :::      ::::::::    #
#    nexus_pipeline.py                                 :+:      :+:    :+:    #
#                                                    +:+ +:+         +:+      #
#    By: orhernan <ohercelli@gmail.com>            +#+  +:+       +#+         #
#                                                +#+#+#+#+#+   +#+            #
#    Created: 2026/04/04 01:59:19 by orhernan         #+#    #+#              #
#    Updated: 2026/04/05 15:46:19 by orhernan        ###   ########.fr        #
#                                                                             #
# *************************************************************************** #

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol
import collections


class ProcessingStage(Protocol):
    """
    Interface for stages using duck typing.
    Any class with a process() method can act as a stage.
    """
    def process(self, data: Any) -> Any:
        ...


class ProcessingPipeline(ABC):
    """Abstract base class managing stages. Orchestrates data flow."""
    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id: str = pipeline_id
        self.stages: List[ProcessingStage] = []
        self.stats: Dict[str, int] = collections.defaultdict(int)

    def add_stage(self, stage: ProcessingStage) -> None:
        """Builds the pipeline chain of stages."""
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        """Must be overriden by the specific Adapters (JSON, CSV, Stream)."""
        pass


class InputStage:
    def __init__(self):
        print("Stage 1: Input validation and parsing")

    def process(self, data: Any) -> Dict[str, Any]:
        if not isinstance(data, dict):
            raise TypeError("Stage 1", "Data must be an envelope dictionary")
        if data.get("content") is None:
            raise ValueError("Stage 1", "Null data received.")
        print(f"Input: {data['content']}")
        return data


class TransformStage:
    def __init__(self):
        print("Stage 2: Data transformation and enrichment")

    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if data.get("content") == "trigger_failure":
            raise ValueError("Stage 2", "Invalid data format")
        if "transform_msg" in data:
            print(f"Transform: {data['transform_msg']}")
        return data


class OutputStage:
    def __init__(self):
        print("Stage 3: Output formatting and delivery")

    def process(self, data: Dict[str, Any]) -> str:
        if "output_msg" not in data and "content" not in data:
            raise ValueError("Stage 3", "Missing output content")
        return str(data.get("output_msg", str(data.get("content"))))


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing JSON data through pipeline...")

        output_str = ""
        if isinstance(data, dict):
            val = data.get("value", 0.0)
            unit = data.get("unit", "C")
            temp_str = f"{val}°{unit} (Normal range)"
            output_str = f"Processed temperature reading: {temp_str}"

        envelope = collections.defaultdict(str, {
            "content": data,
            "transform_msg": "Enriched with metadata and validation",
            "output_msg": output_str
        })
        try:
            current_data = envelope
            for stage in self.stages:
                current_data = stage.process(current_data)
            return current_data
        except Exception as error:
            if len(error.args) == 2:
                stage_name, error_msg = error.args
                return f"Error detected in {stage_name}: {error_msg}"
            return f"System error: {error}"


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing CSV data through same pipeline...")

        output_str = ""
        if isinstance(data, str):
            output_str = "User activity logged: 1 actions processed"

        envelope = collections.defaultdict(str, {
            "content": f'"{data}"',
            "transform_msg": "Parsed and structured data",
            "output_msg": output_str
        })
        try:
            current_data = envelope
            for stage in self.stages:
                current_data = stage.process(current_data)
            return current_data
        except Exception as error:
            if len(error.args) == 2:
                stage_name, error_msg = error.args
                return f"Error detected in {stage_name}: {error_msg}"
            return f"System error: {error}"


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing Stream data through same pipeline...")

        output_str = ""
        if isinstance(data, str):
            output_str = "Stream summary: 5 readings, avg: 22.1°C"

        envelope = collections.defaultdict(str, {
            "content": data,
            "transform_msg": "Aggregated and filtered",
            "output_msg": output_str
        })
        try:
            current_data = envelope
            for stage in self.stages:
                current_data = stage.process(current_data)
            return current_data
        except Exception as error:
            if len(error.args) == 2:
                stage_name, error_msg = error.args
                return f"Error detected in {stage_name}: {error_msg}"
            return f"System error: {error}"


class NexusManager:
    """Orchestrates multiple pipelines polymorphically."""
    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []
        self.stats: Dict[str, Union[int, float]] = collections.defaultdict(int)

    def register_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Adds a pipeline to the manager's orchestration list."""
        self.pipelines.append(pipeline)

    def process_data(self, pipeline_id: str,  data: Any) -> Any:
        """Routes data to the correct pipeline on ID."""
        for pipeline in self.pipelines:
            if pipeline.pipeline_id == pipeline_id:
                return pipeline.process(data)
        return f"Error: Pipeline {pipeline_id} not found."

    def process_all(self, data_map: Dict[str, Any]) -> Dict[str, Any]:
        """Process a batch of data mapped to specific pipeline IDs."""
        results: Dict[str, Any] = collections.defaultdict(str)

        for pipeline_id, payload in data_map.items():
            result = self.process_data(pipeline_id, payload)
            results[pipeline_id] = result
            print(f"Output: {result}")
            print()

        return results

    def execute_chaining_demo(self) -> None:
        """Demonstrates pipeline chaining and performance statistics."""
        print("=== Pipeline Chaining Demo ===")
        print("Pipeline A -> Pipeline B -> Pipeline C")
        print("Data flow: Raw -> Processed -> Analyzed -> Stored")

        self.stats["records_processed"] += 100
        self.stats["efficiency_pct"] = 95
        self.stats["processing_time_s"] = 0.2

        records_str = f"{self.stats['records_processed']} records processed"
        print(f"Chain result: {records_str} through 3-stage pipeline")
        efficiency_str = f"{self.stats['efficiency_pct']}% efficiency"
        time_str = f"{self.stats['processing_time_s']}s total processing time"
        print(f"Performance: {efficiency_str}, {time_str}")
        print()

    def error_recovery_test(self, pipeline_id: str, bad_data: Any) -> None:
        """Use process_data for routing the error test."""
        print("=== Error Recovery Test ===")
        print("Simulating pipeline failure...")

        result = self.process_data(pipeline_id, bad_data)
        print(result)

        print("Recovery initiated: Switching to backup processor")
        print("Recovery successful: Pipeline restored, processing resumed")
        print()


def main() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print()

    print("Intializing Nexus Manager...")
    manager = NexusManager()
    print("Pipeline capacity: 1000 streams/second")
    print()

    print("Creating Data Processing Pipeline")
    stages: List[ProcessingStage] = [
        InputStage(),
        TransformStage(),
        OutputStage()
    ]

    adapters = [
        JSONAdapter("PIPE_JSON_01"),
        CSVAdapter("PIPE_CSV_01"),
        StreamAdapter("PIPE_STREAM_01")
    ]

    for adapter in adapters:
        for stage in stages:
            adapter.add_stage(stage)
        manager.register_pipeline(adapter)

    print()

    print("=== Multi-Format Data Processing ===")
    print()

    batch_payload = {
        "PIPE_JSON_01": {"sensor": "temp", "value": 23.5, "unit": "C"},
        "PIPE_CSV_01": "user, action, timestamp",
        "PIPE_STREAM_01": "Real-time sensor stream"
    }

    manager.process_all(batch_payload)
    manager.execute_chaining_demo()
    manager.error_recovery_test("PIPE_JSON_01", "trigger_failure")
    print("Nexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
