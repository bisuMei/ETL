from dataclasses import dataclass
from datetime import datetime
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
