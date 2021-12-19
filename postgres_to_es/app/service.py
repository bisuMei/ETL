"""Service to load data from postgres to elasticsearch."""
import logging

from collections import defaultdict
from datetime import date
from typing import List, Tuple

from backoff import backoff

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from postgres_data_query import (
    filmworks_additional_query,
    filmworks_by_genre,
    filmworks_data_query,
    filmworks_persons_query,
    genres_query,
    genres_data_query,
    persons_query,
    persons_data_query,
)
from postgres_schemas import MovieData, PersonFilm, GenreData, PersonsData, FilmworkSchema, GenreSchema, PersonSchema
from state_saver import JsonFileStorage, State

logger = logging.getLogger()

BASE_STATE = date.min.strftime('%Y-%m-%d %X')


class PostgresLoaderService:
    """Save data to postgres."""

    states_after_save = {}

    def __init__(self, connection):
        self.connection = connection
        self.cursor = self.connection.cursor()
        self.storage = JsonFileStorage('state_config.json')
        self.state_loader = State(self.storage)

    @backoff()
    def load_filmworks_data(self) -> Tuple[List[MovieData], List[PersonFilm]]:
        """Load raw data from postgres."""

        genres_state = self.state_loader.get_state('genres_state')
        if not genres_state:
            genres_state = BASE_STATE

        self.cursor.execute(genres_query.format(genres_state))
        genres_data = self.cursor.fetchall()
        genres_ids = tuple([genre_id[0] for genre_id in genres_data])

        if genres_ids:
            new_genre_state = str(genres_data[0][1])
            final_genre_state = str(genres_data[-1][1])
            self.states_after_save['genres_state'] = final_genre_state

            self.state_loader.set_state('genres_state', new_genre_state)
            self.cursor.execute(filmworks_by_genre.format(genres_ids))
        filmworks_data_genres = self.cursor.fetchall()
        filmworks_ids_changed_genres = tuple([filmwork_id[0] for filmwork_id in filmworks_data_genres])

        persons_state = self.state_loader.get_state('persons_state')
        if not persons_state:
            persons_state = BASE_STATE

        self.cursor.execute(persons_query.format(persons_state))
        persons_data = self.cursor.fetchall()
        persons_ids = tuple([person_id[0] for person_id in persons_data])

        if persons_ids and filmworks_ids_changed_genres:
            new_person_state = str(persons_data[0][1])
            final_person_state = str(persons_data[-1][1])
            self.states_after_save['persons_state'] = final_person_state
            self.state_loader.set_state('persons_state', new_person_state)

            self.cursor.execute(
                filmworks_data_query.format(persons_ids=persons_ids, filmworks_ids=filmworks_ids_changed_genres),
            )
        elif persons_ids:
            new_person_state = str(persons_data[0][1])
            final_person_state = str(persons_data[-1][1])
            self.states_after_save['persons_state'] = final_person_state
            self.state_loader.set_state('persons_state', new_person_state)

            self.cursor.execute(filmworks_persons_query.format(persons_ids=persons_ids))

        filmworks_data_persons = self.cursor.fetchall()
        filmworks_ids_changed_persons = tuple(set([filmwork_id[0] for filmwork_id in filmworks_data_persons]))
        person_film_data = [PersonFilm(*item) for item in filmworks_data_persons]

        final_filmworks_ids = tuple(set(filmworks_ids_changed_genres + filmworks_ids_changed_persons))

        if final_filmworks_ids:
            self.cursor.execute(filmworks_additional_query.format(filmworks_ids=final_filmworks_ids))
        additional_film_data = self.cursor.fetchall()
        film_work_data = [MovieData(*item) for item in additional_film_data]

        return film_work_data, person_film_data

    def load_genres_data(self):
        """Load genres data from postgres."""
        genres_data_state = self.state_loader.get_state('genres_data_state')
        if not genres_data_state:
            genres_data_state = BASE_STATE
        self.cursor.execute(genres_data_query.format(genres_data_state))
        raw_genres_data = self.cursor.fetchall()
        if raw_genres_data:
            new_genre_data_state = str(raw_genres_data[0][-1])
            final_genre_data_state = str(raw_genres_data[-1][-1])
            self.states_after_save['genres_data_state'] = final_genre_data_state
            self.state_loader.set_state('genres_data_state', new_genre_data_state)

        return [GenreData(*item) for item in raw_genres_data]

    def load_persons_data(self):
        """Load persons data from postgres."""
        persons_data_state = self.state_loader.get_state('persons_data_state')
        if not persons_data_state:
            persons_data_state = BASE_STATE
        self.cursor.execute(persons_data_query.format(persons_data_state))
        raw_persons_data = self.cursor.fetchall()
        if raw_persons_data:
            new_persons_state = str(raw_persons_data[0][-1])
            final_persons_data_state = str(raw_persons_data[-1][-1])
            self.states_after_save['persons_data_state'] = final_persons_data_state
            self.state_loader.set_state('persons_data_state', new_persons_state)

        return [PersonsData(*item) for item in raw_persons_data]


class ElasticSaverService:
    """Save data from postgres to elastic."""

    def __init__(self):
        self.storage = JsonFileStorage('state_config.json')
        self.state_loader = State(self.storage)

    @backoff()
    def create_index(self, es: Elasticsearch, index_name: str, index_settings: dict) -> bool:
        """Create index if not exist."""

        created = False

        try:
            if not es.indices.exists(index=index_name):
                # Ignore 400 means to ignore "Index Already Exist" error.
                es.indices.create(index=index_name, ignore=400, body=index_settings)
                logger.info('Index created')
            created = True
        except Exception as ex:
            logger.exception("Something went wrong in create index: %s", str(ex))
        finally:
            return created

    @backoff()
    def store_record(self, es: Elasticsearch, index_name: str, record: dict):
        try:
            outcome = es.index(index=index_name, document=record)
            return outcome
        except Exception as ex:
            logger.exception('Error in indexing data: %s', str(ex))

    def gendata(self, index_name: str, docs: List[dict]) -> dict:
        for doc in docs:
            yield {
                "_index": index_name,
                "_id": doc['id'],
                "_source": doc
            }

    @backoff()
    def bulk_store(self, es: Elasticsearch, index_name: str, list_of_record: List[dict], states: dict = None) -> None:
        try:
            bulk(es, self.gendata(index_name, list_of_record))
        except Exception as ex:
            logger.exception('Error in indexing data: %s', str(ex))

        logger.info('Success load to elastic. Start saving states..')
        for state_key, state_value in states.items():
            self.state_loader.set_state(state_key, state_value)


class TransformDataService:
    """Data transformer from raw postgres to elastic loader."""

    def transform_filmworks_data(
        self,
        film_work_data: List[MovieData],
        person_film_data: List[PersonFilm],
    ) -> List[dict]:
        """Transform raw data from postgres to elastic format."""

        person_data = defaultdict(list)
        result = []
        for pers in person_film_data:
            person_info = {'id': pers.person_id, 'role': pers.role, 'full_name': pers.full_name}
            person_data[pers.film_id].append(person_info)

        for film in film_work_data:
            writers = []
            actors = []
            actors_names = []
            writers_names = []

            movie = {
                'id': film.id,
                'imdb_rating': film.rating,
                'description': film.description,
                'title': film.title,
                'genre': film.genres,
            }

            if persons := person_data.get(film.id):
                for person in persons:
                    role = person['role']
                    if role == 'director':
                        movie['director'] = person['full_name']
                    elif role == 'writer':
                        writers.append({'id': person['id'], 'name': person['full_name']})
                        writers_names.append(person['full_name'])
                    elif role == 'actor':
                        actors.append({'id': person['id'], 'name': person['full_name']})
                        actors_names.append(person['full_name'])
            movie['actors'] = actors
            movie['actors_names'] = actors_names
            movie['writers'] = writers
            movie['writers_names'] = writers_names
            if not movie.get('director'):
                movie['director'] = None
            result.append(FilmworkSchema.parse_obj(movie).dict())

        return result

    def transform_genres_data(self, genres_data: List[GenreData]) -> List[dict]:
        """Transform genres data to load to elastic."""

        result = []
        for genre in genres_data:
            genres_info = {'id': genre.id, 'name': genre.name, 'description': genre.description}
            result.append(GenreSchema.parse_obj(genres_info).dict())
        return result

    def transform_persons_data(self, persons_data: List[PersonsData]) -> List[dict]:
        """Transform persons data to load to elastic."""

        result = []
        person_roles = defaultdict(set)
        for person in persons_data:
            person_roles[person.id].add(person.role)

        for person in persons_data:
            roles = list(person_roles.get(person.id))
            persons_info = {'id': person.id, 'full_name': person.full_name, 'role': roles}
            result.append(PersonSchema.parse_obj(persons_info).dict())
        return result
