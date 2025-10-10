FROM python:3.13

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libxml2-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /app/requirements.txt

COPY ./.env /root/.env

ARG GIT_USERNAME
ARG GIT_TOKEN

RUN sed -i "s/\${GIT_USERNAME}/${GIT_USERNAME}/g" requirements.txt && \
    sed -i "s/\${GIT_TOKEN}/${GIT_TOKEN}/g" requirements.txt

RUN git config --global credential.helper store && \
    echo "https://${GIT_USERNAME}:${GIT_TOKEN}@github.com" > ~/.git-credentials

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV nome=""

ENTRYPOINT ["sh", "-c", "python main.py \"$nome\" \"$ano\""]