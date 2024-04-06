from geopy import distance
from visionapi.messages_pb2 import GeoCoordinate


class TrackedObject:
    def __init__(
        self, id: str, geo_coordinate: GeoCoordinate, frame_timestamp: int
    ) -> None:
        self.id = id
        self.last_update_at = frame_timestamp
        self.current_coordinate: GeoCoordinate = geo_coordinate
        self.previous_coordinate: GeoCoordinate | None = None
        self.velocity: float | None = None

    def update(self, new_coordinate: GeoCoordinate, frame_timestamp: int) -> None:
        self.previous_coordinate = self.current_coordinate
        self.current_coordinate = new_coordinate
        distance_in_m = distance.distance(
            (self.previous_coordinate.latitude, self.previous_coordinate.longitude),
            (self.current_coordinate.latitude, self.current_coordinate.longitude),
        ).m
        time_diff_in_s = (frame_timestamp - self.last_update_at) / 1000
        self.velocity = [
            0 if v < 1 else v for v in [(distance_in_m / time_diff_in_s * 3.6)]
        ][0]
        self.last_update_at = frame_timestamp

    def to_json(self):
        if self.current_coordinate.latitude is None or self.current_coordinate.longitude is None:
            raise ValueError("Latitude and longitude must be set")
        return {
            "id": self.id,
            "velocity": self.velocity,
            "coordinates": {
                "lat": self.current_coordinate.latitude,
                "lng": self.current_coordinate.longitude,
            },
        }
