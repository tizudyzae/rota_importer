ARG BUILD_FROM
FROM $BUILD_FROM

ENV LANG=C.UTF-8

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt

RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-pandas \
    py3-lxml \
    gcc \
    musl-dev \
    python3-dev \
    libffi-dev \
    jpeg-dev \
    zlib-dev \
    freetype-dev \
    tesseract-ocr \
    poppler-utils \
    && ln -sf python3 /usr/bin/python \
    && pip3 install --no-cache-dir --break-system-packages -r /tmp/requirements.txt

COPY run.sh /run.sh
COPY app /app
COPY ha_bridge_component /app/ha_bridge_component

RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
