FROM python:3.10 AS base
WORKDIR /src

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install sqlite-utils==3.26.1 \
    pip install pandas==1.5.0 \ 
    pip install sqlalchemy \
    pip install tqdm

ADD src/mloutput2sql.py .
COPY --chmod=755 mloutput2sql.sh ./
