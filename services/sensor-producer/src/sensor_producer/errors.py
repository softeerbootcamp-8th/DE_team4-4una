"""Expected trip-level simulation failures."""

from enum import StrEnum


class TripSkipReason(StrEnum):
    PICKUP_ZONE_NOT_FOUND = "PICKUP_ZONE_NOT_FOUND"
    DROPOFF_ZONE_NOT_FOUND = "DROPOFF_ZONE_NOT_FOUND"
    PICKUP_ZONE_NO_ROUTABLE_NODES = "PICKUP_ZONE_NO_ROUTABLE_NODES"
    DROPOFF_ZONE_NO_ROUTABLE_NODES = "DROPOFF_ZONE_NO_ROUTABLE_NODES"
    NO_DIRECTED_ROUTE = "NO_DIRECTED_ROUTE"
    SPEED_PROFILE_INFEASIBLE = "SPEED_PROFILE_INFEASIBLE"
    EMPTY_SENSOR_STREAM = "EMPTY_SENSOR_STREAM"


class TripInfeasibleError(ValueError):
    def __init__(self, reason: TripSkipReason, detail: str):
        self.reason = reason
        super().__init__(detail)
