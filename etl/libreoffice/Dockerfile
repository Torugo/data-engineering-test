FROM python:3.7.15-bullseye

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
        

RUN apt install -y default-jre \
        libreoffice-java-common \
        libreoffice-common \
        libreoffice-calc \
        unoconv

ARG AIRFLOW_HOME=/opt/airflow

RUN pip install psycopg2-binary
RUN pip install "apache-airflow[redis,celery]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"
RUN pip uninstall pyarrow
RUN pip install pandas fastparquet odfpy
RUN apt-get purge --auto-remove -yqq \
    && apt-get autoremove -yqq --purge \
    && apt-get clean 

