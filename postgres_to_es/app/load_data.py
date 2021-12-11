import logging.config
from os import path

from elasticsearch import Elasticsearch
from psycopg2 import connect
from psycopg2.extras import DictCursor
from YamJam import yamjam

from backoff import backoff
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


if __name__ == '__main__':
    log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
    logging.config.fileConfig(log_file_path)

    pg_conn = connect_to_postgres()
    postgres_service = PostgresLoaderService(pg_conn)

    es = connect_elastic()
    service = ElasticSaverService()
    service.create_index(es, 'movies')

    transform_service = TransformDataService()

    while True:
        data_from_postgres = postgres_service.load_data()
        data_to_elastic = transform_service.transform_data_to_elastic(*data_from_postgres)
        if not data_to_elastic:
            break
        logger.info("Get data from postgres. Transformed to save to elastic..")
        service.bulk_store(es, 'movies', data_to_elastic, postgres_service.states_after_save)
