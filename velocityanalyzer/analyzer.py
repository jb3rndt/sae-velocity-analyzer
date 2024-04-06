import logging
from queue import Queue
from typing import Any, Callable, Dict

from prometheus_client import Counter, Histogram, Summary
from visionapi.messages_pb2 import SaeMessage

from velocityanalyzer.tracked_object import PositionUpdate, TrackedObject

from .config import AnalyzerConfig

logging.basicConfig(
    format="%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s"
)
logger = logging.getLogger(__name__)

GET_DURATION = Histogram(
    "velocity_analyzer_get_duration",
    "The time it takes to deserialize the proto until returning the tranformed result as a serialized proto",
    buckets=(0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25),
)
OBJECT_COUNTER = Counter(
    "velocity_analyzer_object_counter", "How many detections have been transformed"
)
PROTO_SERIALIZATION_DURATION = Summary(
    "velocity_analyzer_proto_serialization_duration",
    "The time it takes to create a serialized output proto",
)
PROTO_DESERIALIZATION_DURATION = Summary(
    "velocity_analyzer_proto_deserialization_duration",
    "The time it takes to deserialize an input proto",
)


class Analyzer:
    def __init__(self, config: AnalyzerConfig, push_update: Callable) -> None:
        self.config = config
        self.objects: Dict[str, TrackedObject] = {}
        logger.setLevel(self.config.log_level.value)
        self.push_update = push_update

    def __call__(self, input_proto) -> Any:
        return self.get(input_proto)

    @GET_DURATION.time()
    def get(self, input_proto, **kwargs):
        sae_msg = self._unpack_proto(input_proto)

        frame_timestamp = sae_msg.frame.timestamp_utc_ms

        for detection in sae_msg.detections:
            object_id = detection.object_id.hex()
            if object_id in self.objects:
                self.objects[object_id].update(
                    PositionUpdate(detection.geo_coordinate, frame_timestamp)
                )
            else:
                self.objects[object_id] = TrackedObject(
                    object_id, detection.geo_coordinate, frame_timestamp
                )
            detection.confidence = self.objects[object_id].velocity or 0

        self.objects = {
            k: v
            for k, v in self.objects.items()
            if frame_timestamp - v.last_positions[-1].timestamp < 2000
        }

        self.push_update({"data": [obj.to_json() for _, obj in self.objects.items()]})

        return self._pack_proto(sae_msg)

    @PROTO_DESERIALIZATION_DURATION.time()
    def _unpack_proto(self, sae_message_bytes):
        sae_msg = SaeMessage()
        sae_msg.ParseFromString(sae_message_bytes)

        return sae_msg

    @PROTO_SERIALIZATION_DURATION.time()
    def _pack_proto(self, sae_msg: SaeMessage):
        return sae_msg.SerializeToString()
