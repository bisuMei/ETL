"""Get raw data from postgres."""

genres_query = """
                    SELECT id, updated_at
                    FROM content.genre
                    WHERE updated_at > '{0}'
                    ORDER BY updated_at
                    LIMIT 100;
                """

filmworks_by_genre = """
                        SELECT gfw.film_work_id
                        FROM content.genre_film_work gfw
                        INNER JOIN content.film_work fw ON gfw.film_work_id = fw.id
                        WHERE gfw.genre_id IN {0}
                        ORDER BY fw.updated_at;
                    """

persons_query = """
            SELECT id, updated_at
            FROM content.person
            WHERE updated_at > '{0}'
            ORDER BY updated_at
            LIMIT 100;
        """

filmworks_data_query = """
                SELECT pfw.film_work_id, pfw.person_id, pfw.role, prs.full_name, fw.updated_at
                FROM content.person_film_work pfw
                INNER JOIN content.person prs ON (pfw.person_id = prs.id)
                INNER JOIN content.film_work fw ON (pfw.film_work_id = fw.id)
                WHERE pfw.person_id IN {persons_ids} OR fw.id IN {filmworks_ids}
                ORDER BY fw.updated_at;
            """

filmworks_persons_query = """
                SELECT pfw.film_work_id, pfw.person_id, pfw.role, prs.full_name, fw.updated_at
                FROM content.person_film_work pfw
                INNER JOIN content.person prs ON (pfw.person_id = prs.id)
                INNER JOIN content.film_work fw ON (pfw.film_work_id = fw.id)
                WHERE pfw.person_id IN {persons_ids}
                ORDER BY fw.updated_at;
            """

filmworks_additional_query = """
            SELECT fw.id,
                   fw.title,
                   fw.description,
                   fw.rating,
                   ARRAY_AGG(DISTINCT content.genre.name)
            FROM content.film_work fw
             LEFT OUTER JOIN content.genre_film_work gfw ON (fw.id = gfw.film_work_id)
             LEFT OUTER JOIN content.genre ON (gfw.genre_id = content.genre.id)
             WHERE fw.id IN {filmworks_ids}
             GROUP BY fw.id;
        """

genres_data_query = """
                    SELECT id, name, description, updated_at
                    FROM content.genre
                    WHERE updated_at > '{0}'
                    ORDER BY updated_at
                    LIMIT 100;
                """

persons_data_query = """
                    SELECT pfw.person_id, p.full_name, pfw.role, p.updated_at
                    FROM content.person_film_work pfw
                    INNER JOIN content.person p ON (pfw.person_id = p.id)
                    WHERE p.updated_at > '{0}'
                    ORDER BY p.updated_at
                    LIMIT 100;
                """