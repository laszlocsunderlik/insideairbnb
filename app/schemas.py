from datetime import datetime
from typing import Optional, List, Union, Tuple

from pydantic import BaseModel
from geojson import MultiPolygon, Point


class Neighbourhoods(BaseModel):
    id: int
    neighbourhood: str
    neighbourhood_group: Optional[str] = None
    download_date: str


class GeometryListings(BaseModel):
    latitude: float
    longitude: float

class Listings(BaseModel):
    id: int
    name: str
    host_id: int
    host_name: str
    neighbourhood_group: Optional[str] = None
    neighbourhood: str
    latitude: float
    longitude: float
    room_type: str
    price: int
    minimum_nights: int
    number_of_reviews: Optional[int] = None
    last_review: Optional[datetime] = None
    reviews_per_month: Optional[float] = None
    calculated_host_listings_count: int
    availability_365: int
    number_of_reviews_ltm: int
    license: Optional[str] = None
    download_date: datetime


class GeometryNeighbourhoods(BaseModel):
    srid: int
    type: str
    geometry: MultiPolygon

    class Config:
        arbitrary_types_allowed = True


class MyFeature(BaseModel):
    type: str = "Feature"
    geometry: Union[GeometryListings, GeometryNeighbourhoods]
    properties: Union[Listings, Neighbourhoods]


class MyFeatureCollection(BaseModel):
    type: str = "FeatureCollection"
    features: List[MyFeature]


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
