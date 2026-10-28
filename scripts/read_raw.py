"""Минимальная проверка чтения raw-файла через DuckDB."""

import argparse
import os

import duckdb


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    args = parser.parse_args()

    endpoint = os.environ["S3_ENDPOINT"]
    access_key = os.environ["S3_ACCESS_KEY"]
    secret_key = os.environ["S3_SECRET_KEY"]

    connection = duckdb.connect()

    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")

    connection.execute(f"SET s3_endpoint='{endpoint}'")
    connection.execute("SET s3_url_style='path'")
    connection.execute("SET s3_use_ssl=false")
    connection.execute("SET s3_region='us-east-1'")
    connection.execute(f"SET s3_access_key_id='{access_key}'")
    connection.execute(f"SET s3_secret_access_key='{secret_key}'")

    result = connection.execute(
    f"""
    SELECT *
    FROM read_csv_auto(
        '{args.path}',
        encoding='latin-1'
    )
    LIMIT 5
    """
    )

    columns = [column[0] for column in result.description]
    rows = result.fetchall()

    print(" | ".join(columns))

    for row in rows:
        print(" | ".join(str(value) for value in row))


if __name__ == "__main__":
    main()
