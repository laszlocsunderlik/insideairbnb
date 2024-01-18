from datetime import date, datetime
from typing import Optional, List

from shapely.geometry import MultiPolygon
from pydantic import BaseModel, BaseConfig, field_validator


class Coordinates(BaseModel):
    latitude: float = 0
    longitude: float = 0

    @field_validator("latitude")
    def lat_within_range(cls, v):
        if not -90 <= v <= 90:
            raise ValueError("Latitude outside allowed range")
        return v

    @field_validator("longitude")
    def lng_within_range(cls, v):
        if not -180 <= v <= 180:
            raise ValueError("Longitude outside allowed range")
        return v


class Listings(BaseModel):
    id: int
    name: str
    host_id: int
    host_name: str
    neighbourhood_group: Optional[str]
    neighbourhood: str
    room_type: str
    price: int
    minimum_nights: int
    number_of_reviews: Optional[int]
    last_review: Optional[str]
    reviews_per_month: Optional[float]
    calculated_host_listings_count: int
    availability_365: int
    number_of_reviews_ltm: int
    license: Optional[str]
    download_date: str


class GeometryListings(BaseModel):
    type: str
    coordinates: List[float]


class Feature(BaseModel):
    type: str = "Feature"
    geometry: GeometryListings
    properties: Listings


class FeatureCollection(BaseModel):
    type: str = "FeatureCollection"
    features: List[Feature]


class Neighbourhoods(BaseModel):
    class Config:
        arbitrary_types_allowed = True
    id: int
    neighbourhood: str
    neighbourhood_group: str
    download_date: date
    geometry: MultiPolygon



class UserCreate(BaseModel):
    username: str
    password: str


class UserLogin(BaseModel):
    username: str
    password: str


class UserOut(BaseModel):
    id: int
    username: str
    created_at: datetime

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    id: Optional[int] = None
