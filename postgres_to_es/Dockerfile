FROM python:3.9

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apt-get update && apt-get -y install cron vim
COPY . .
COPY crontab /etc/cron.d/crontab
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

RUN crontab crontab

CMD ["cron", "-f"]
