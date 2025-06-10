#!/usr/bin/python3
import os
from logging.config import dictConfig


from flask import Flask, jsonify, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

#TODO: set up rate limiter

app = Flask(__name__)
app.config.from_prefixed_env()
log = app.logger


DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@postgres/postgres")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={
        "autocommit": True,
        "row_factory": namedtuple_row,
    },
    min_size=4, 
    max_size=10,
    open=True,
    # check=ConnectionPool.check_connection,
    name="postgres_pool",
    timeout=5,
)

@app.route("/ping", methods=("GET",))
def ping():
    log.debug("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!")
    return jsonify({"message": "pong!"}), 200


@app.route("/")
def airports():
    """
    Returns all airports (name and city).
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                airports = cur.execute(
                    """
                    SELECT nome, cidade
                    FROM aeroporto;
                    """
                ).fetchall()
                log.debug(f"{cur.rowcount} airports found.")
                    
                return jsonify(airports), 200
            except Exception as e:
                return jsonify({"error:": str(e)}), 400




if __name__ == "__main__":
    app.run()

