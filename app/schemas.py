from datetime import date, datetime
from typing import Optional

from shapely.geometry import MultiPolygon
from pydantic import BaseModel, EmailStr


class Listings(BaseModel):
    id: int
    name: str
    host_id: int
    host_name: str
    neighbourhood_group: str
    neighbourhood: str
    latitude: float
    longitude: float
    room_type: str
    price: int
    minimum_nights: int
    number_of_reviews: int
    last_review: date
    reviews_per_month: float
    calculated_host_listings_count: int
    availability_365: int
    number_of_reviews_ltm: int
    license: str
    download_date: date


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
