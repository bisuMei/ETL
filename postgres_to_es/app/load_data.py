import logging.config
from os import path

from elasticsearch import Elasticsearch
from psycopg2 import connect
from psycopg2.extras import DictCursor
from YamJam import yamjam

from backoff import backoff
from elastic_schema import genres_index_schema, filmworks_index_schema, persons_index_schema
from service import ElasticSaverService, PostgresLoaderService, TransformDataService

logger = logging.getLogger()


@backoff()
def connect_elastic():
    es_dsn = yamjam()['elastic']['envs']
    es = Elasticsearch([es_dsn])
    if es.ping():
        logger.info("Success connect to elastic")
        return es
    else:
        logger.info("Can not connect to elastic, retry later.")
        raise Exception


@backoff()
def connect_to_postgres():
    postgres_dsn = yamjam()['movies']['database']
    try:
        pg_conn = connect(**postgres_dsn, cursor_factory=DictCursor)
        logger.info("Success connect to postgres.")
        return pg_conn
    except Exception:
        logger.info("Can not connect to postgres, retry later.")
        raise


def load_from_postgres_to_elastic(pg_conn, es):
    """Load data from postgres, transform and send to elastic."""

    postgres_service = PostgresLoaderService(pg_conn)

    service = ElasticSaverService()
    service.create_index(es, 'movies', filmworks_index_schema)
    service.create_index(es, 'genres', genres_index_schema)
    service.create_index(es, 'persons', persons_index_schema)

    transform_service = TransformDataService()

    while True:
        data_from_postgres = postgres_service.load_filmworks_data()
        data_to_elastic = transform_service.transform_filmworks_data(*data_from_postgres)
        if not data_to_elastic:
            break
        logger.info("Get filmworks data from postgres. Transformed to save to elastic..")
        service.bulk_store(es, 'movies', data_to_elastic, postgres_service.states_after_save)
    while True:
        genres_data_from_postgres = postgres_service.load_genres_data()
        genres_data_to_elastic = transform_service.transform_genres_data(genres_data_from_postgres)
        if not genres_data_to_elastic:
            break
        logger.info("Get genres data from postgres. Transformed to save to elastic..")
        service.bulk_store(es, 'genres', genres_data_to_elastic, postgres_service.states_after_save)
    while True:
        persons_data_from_postgres = postgres_service.load_persons_data()
        if not persons_data_from_postgres:
            break
        persons_data_to_elastic = transform_service.transform_persons_data(*persons_data_from_postgres)
        logger.info("Get persons data from postgres. Transformed to save to elastic..")
        service.bulk_store(es, 'persons', persons_data_to_elastic, postgres_service.states_after_save)


if __name__ == '__main__':
    log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
    logging.config.fileConfig(log_file_path)

    pg_conn = connect_to_postgres()

    es = connect_elastic()

    load_from_postgres_to_elastic(pg_conn, es)
