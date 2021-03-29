FROM python:3.9-alpine AS python-image

FROM python-image AS dependencies

ENV PYTHONUNBUFFERED 1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on
RUN apk add --no-cache --update --virtual=build-deps musl-dev gcc librdkafka \
librdkafka-dev


FROM dependencies AS base

RUN mkdir /producers
WORKDIR /producers
COPY requirements.txt /producers/


FROM base AS development

RUN pip install -r requirements.txt
