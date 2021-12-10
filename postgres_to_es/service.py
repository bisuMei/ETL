"""Service to load data from postgres to elasticsearch."""
import logging
from collections import defaultdict

from elasticsearch.helpers import bulk

from elastic_schema import index_data
from postgres_data_query import (
    filmworks_additional_query,
    filmworks_by_genre,
    filmworks_data_query,
    filmworks_persons_query,
    genres_query,
    persons_query
)
from postgres_schemas import MovieData, PersonFilm
from state_saver import JsonFileStorage, State

logger = logging.getLogger()

BASE_STATE = '1900-06-16 20:14:09.309735 +00:00'


class PostgresLoaderService:
    """Save data to postgres."""

    states_after_save = {}

    def __init__(self, connection):
        self.connection = connection
        self.cursor = self.connection.cursor()
        self.storage = JsonFileStorage('state_config.json')
        self.state_loader = State(self.storage)

    def load_data(self):
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


class ElasticSaverService:
    """Save data from postgres to elastic."""

    def __init__(self):
        self.storage = JsonFileStorage('state_config.json')
        self.state_loader = State(self.storage)

    def create_index(self, es, index_name):
        """Create index if not exist."""

        created = False

        try:
            if not es.indices.exists(index=index_name):
                # Ignore 400 means to ignore "Index Already Exist" error.
                es.indices.create(index=index_name, ignore=400, body=index_data)
                logger.info('Index created')
            created = True
        except Exception as ex:
            logger.exception("Something went wrong in create index: %s", str(ex))
        finally:
            return created

    def store_record(self, es, index_name, record):
        try:
            outcome = es.index(index=index_name, document=record)
            return outcome
        except Exception as ex:
            logger.exception('Error in indexing data: %s', str(ex))

    def gendata(self, index_name, docs):
        for doc in docs:
            yield {
                "_index": index_name,
                "_id": doc['id'],
                "_source": doc
            }

    def bulk_store(self, es, index_name, list_of_record, states: dict = None) -> None:
        try:
            bulk(es, self.gendata(index_name, list_of_record))
        except Exception as ex:
            logger.exception('Error in indexing data: %s', str(ex))

        logger.info('Success load to elastic. Start saving states..')
        for state_key, state_value in states.items():
            self.state_loader.set_state(state_key, state_value)


class TransformDataService:
    """Data transformer from raw postgres to elastic loader."""

    def transform_data_to_elastic(
        self,
        film_work_data,
        person_film_data,
    ):
        """Transform raw data from postgres to elastic format."""

        person_data = defaultdict(list)
        result = []
        for pers in person_film_data:
            person_info = {}
            person_info['id'] = pers.person_id
            person_info['role'] = pers.role
            person_info['full_name'] = pers.full_name
            person_data[pers.film_id].append(person_info)

        for film in film_work_data:
            movie = {}
            writers = []
            actors = []
            actors_names = []
            writers_names = []

            movie['id'] = film.id
            movie['imdb_rating'] = film.rating
            movie['description'] = film.description
            movie['title'] = film.title
            movie['genre'] = film.genres
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
            result.append(movie)

        return result
