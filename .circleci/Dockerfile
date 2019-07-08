FROM python:3.7.3-alpine3.10

WORKDIR /app

ENV PYTHONPATH=/app
ENV POSTGRESQL_PASSWORD ""
ENV POSTGRESQL_USERNAME ""
ENV POSTGRESQL_DATABASE ""
ENV POSTGRESQL_PORT_NUMBER 0
ENV POSTGRESQL_HOST ""

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
COPY asyncpg_migrate ./
COPY tests ./

RUN wget https://raw.githubusercontent.com/eficode/wait-for/master/wait-for -O ./wait-for && \
        chmod +x ./wait-for

ENTRYPOINT ["./wait-for", "${POSTGRESL_HOST}:${POSTGRESL_PORT}", "--"]
CMD ["pytest", "-lv", "tests/integration"]
