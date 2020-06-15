FROM python:3.8.1-alpine

COPY ./requirements.txt ./requirements-dev.txt /app/

WORKDIR /app

RUN apk add --update --no-cache g++ libxslt-dev && \
    pip install -r requirements.txt -r requirements-dev.txt

RUN addgroup spiders && \
    adduser --disabled-password spider && \
    adduser spider spiders

RUN chown -R spider:spiders /app/

USER spider

COPY --chown=spider:spiders . .

CMD ./app
