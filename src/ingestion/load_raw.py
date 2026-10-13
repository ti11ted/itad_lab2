"""Скачать HTTP-файл и сохранить его без изменений в raw."""

from __future__ import annotations

import logging
from typing import Any

import requests

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

RAW_BUCKET = "raw"


def load_to_raw(
    *,
    source_url: str,
    source_filename: str,
    dataset_slug: str,
    s3_conn_id: str,
    **context: Any,
) -> None:
    """Загрузить исходный файл или оставить уже существующий raw-объект."""

    logical_date = context["ds"]

    key = (
        f"{dataset_slug}/"
        f"ingested_on={logical_date}/"
        f"{source_filename}"
    )

    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    if not s3_hook.check_for_bucket(bucket_name=RAW_BUCKET):
        raise AirflowException(
            "Бакет 'raw' недоступен. "
            "Сначала необходимо запустить инфраструктуру."
        )

    if s3_hook.check_for_key(
        key=key,
        bucket_name=RAW_BUCKET,
    ):
        logging.info(
            "Raw object already exists: s3://%s/%s",
            RAW_BUCKET,
            key,
        )
        return

    response = requests.get(
        source_url,
        timeout=60,
    )
    response.raise_for_status()

    s3_hook.load_bytes(
        bytes_data=response.content,
        key=key,
        bucket_name=RAW_BUCKET,
        replace=False,
    )

    logging.info(
        "Downloaded %s to s3://%s/%s",
        source_url,
        RAW_BUCKET,
        key,
    )