from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional


@dataclass
class PersonFilm:

    __slots__ = (
        'film_id',
        'person_id',
        'role',
        'full_name',
        'updated_at',
    )

    film_id: str
    person_id: str
    role: str
    full_name: str
    updated_at: Optional[datetime]


@dataclass
class MovieData:

    __slots__ = (
        'id',
        'title',
        'description',
        'rating',
        'genres',
    )

    id: str
    title: str
    description: str
    rating: float
    genres: List[str]


@dataclass
class GenreData:

    __slots__ = (
        'id',
        'name',
        'description',
        'updated_at',
    )

    id: str
    name: str
    description: Optional[str]
    updated_at: Optional[datetime]


@dataclass
class PersonsData:

    __slots__ = (
        'id',
        'full_name',
        'role',
        'updated_at'
    )

    id: str
    full_name: str
    role: List[str]
    updated_at: Optional[datetime]


class InstanceSchema(BaseModel):

    id: str
    name: str


class FilmworkSchema(BaseModel):

    id: str
    imdb_rating: Optional[float] = None
    genre: List[str]
    title: str
    description: Optional[str] = None
    director: Optional[str] = None
    actors_names: List[str]
    writers_names: List[str]
    actors: List[InstanceSchema]
    writers: List[InstanceSchema]


class GenreSchema(BaseModel):

    id: str
    name: str
    description: Optional[str] = None


class PersonSchema(BaseModel):

    id: str
    full_name: str
    role: List[str]
