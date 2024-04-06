from geopy import distance
from visionapi.messages_pb2 import GeoCoordinate


class PositionUpdate:
    def __init__(self, coordinate: GeoCoordinate, timestamp: int) -> None:
        self.coordinate = coordinate
        self.timestamp = timestamp

    def calc_velocity(self, previous: "PositionUpdate") -> float:
        return (
            distance.distance(
                (previous.coordinate.latitude, previous.coordinate.longitude),
                (self.coordinate.latitude, self.coordinate.longitude),
            ).m
            / ((self.timestamp - previous.timestamp) / 1000)
            * 3.6
        )


class TrackedObject:
    def __init__(
        self, id: str, coordinate: GeoCoordinate, frame_timestamp: int
    ) -> None:
        self.id = id
        self.last_positions = [PositionUpdate(coordinate, frame_timestamp)]
        self.velocity = 0.0

    @property
    def last_position(self) -> PositionUpdate:
        return self.last_positions[-1]

    def update(self, update: PositionUpdate) -> None:
        if len(self.last_positions) >= 20:
            self.velocity += (
                update.calc_velocity(self.last_positions[-1])
                - self.last_positions[1].calc_velocity(self.last_positions[0])
            ) / len(self.last_positions)
            self.last_positions.append(update)
            self.last_positions.pop(0)
        else:
            self.velocity = (
                self.velocity * len(self.last_positions)
                + update.calc_velocity(self.last_positions[-1])
            ) / (len(self.last_positions) + 1)
            self.last_positions.append(update)

    def to_json(self):
        if (
            self.last_position.coordinate.latitude is None
            or self.last_position.coordinate.longitude is None
        ):
            raise ValueError("Latitude and longitude must be set")
        return {
            "id": self.id,
            "velocity": self.velocity,
            "coordinates": {
                "lat": self.last_position.coordinate.latitude,
                "lng": self.last_position.coordinate.longitude,
            },
        }
