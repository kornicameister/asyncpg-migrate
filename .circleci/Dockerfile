FROM python:3.7.3-alpine3.10

WORKDIR /app
ENV PYTHONPATH /app

ENV POSTGRES_PASSWORD ""
ENV POSTGRES_USERNAME ""
ENV POSTGRES_DATABASE ""
ENV POSTGRES_PORT_NUMBER 0
ENV POSTGRES_HOST ""

RUN apk --update \
    add --no-cache --virtual build_dependencies \
        alpine-sdk=1.0-r0 \
        libffi-dev=3.2.1-r6 \
        libressl-dev=2.7.5-r0 \
        libxml2-dev=2.9.9-r2 \
        python3-dev=3.7.3-r0 && \
    pip install --upgrade pip==19.1.1

COPY *requirements.txt ./
COPY requirements/ ./requirements

RUN pip install --no-cache-dir -r requirements.txt -r test-requirements.txt && \
        rm -rf requirements.txt && \
        rm -rf test-requirements.txt

COPY pytest.ini ./
COPY asyncpg_migrate ./asyncpg_migrate
COPY tests ./tests

CMD ["sleep", "5", "&&", "python", "-m", "pytest", "-lv", "tests/integration"]
