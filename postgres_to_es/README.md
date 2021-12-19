Make build:

    docker build --tag cron-app .

To run container with cron job:

    docker run --network=movie_proj -p 8009:8009 cron-app