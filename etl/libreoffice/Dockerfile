FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt update 
RUN apt upgrade -y

RUN apt install -y locales

ENV LANG en_US.UTF-8

RUN apt install -y --no-install-recommends \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat 
        

RUN apt install --no-install-recommends -y default-jre \
        libreoffice-java-common \
        libreoffice-common 

RUN apt install  --no-install-recommends -y python3-dev python3-pip
RUN pip install psycopg2-binary
RUN pip install "apache-airflow[redis,celery]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.8.txt"

RUN apt-get purge --auto-remove -yqq \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \