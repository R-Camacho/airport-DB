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

# TODO: set up rate limiter

app = Flask(__name__)
app.config.from_prefixed_env()
log = app.logger


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@postgres/postgres")

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


@app.route("/", methods=("GET", ))
def airports():
    """
    Returns all airports (name and city).
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                airports = cur.execute(
                    """
                    SELECT nome, cidade, codigo 
                    FROM aeroporto;
                    """,
                    {},
                ).fetchall()
                log.debug(f"{cur.rowcount} airports found.")

                return jsonify(airports), 200
            except Exception as e:
                return jsonify({"error:": str(e)}), 400


@app.route("/voos/<partida>/", methods=("GET", ))
def airport_departures(partida):
    """
    Returns all flights (plane serial number, hour of departure and airport of destination)
    that departure from <partida> up until after 12h
    """

    # TODO: ver o que fazer se o user colocar em letra maiúscula.
    # Ex: se colocar fnc em vez de FNC

    # TODO: maybe dar erro se nao tiver 3 letras ou o erro de cima

    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:

                cur.execute(
                    """
                    SELECT *
                    FROM aeroporto
                    WHERE codigo = %(partida)s;
                    """,
                    {"partida": partida},
                )

                if not cur.fetchone():
                    # TODO: acho que o erro é 404 (not found)
                    # TODO: confirmar todos os códigos de erro em
                    # https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status
                    return jsonify({"error:": f"Airport '{partida}' not found"}, 404)

                departures = cur.execute(
                    # TODO:ver se é para ordenar o output
                    """
                    SELECT v.no_serie, v.hora_partida, v.chegada
                    FROM voo v
                    JOIN aeroporto a_part ON v.partida = a_part.codigo
                    JOIN aeroporto a_cheg ON v.chegada = a_cheg.codigo
                    WHERE a_part.codigo = %(partida)s 
                    AND v.hora_partida BETWEEN NOW() AND NOW() + INTERVAL '12 hours';
                    """,
                    {"partida": partida}
                ).fetchall()

                log.debug(
                    f"{cur.rowcount} departures found in the {partida} airport for the next 12h.")

                return jsonify(departures), 200
            except Exception as e:
                return jsonify({"error:": str(e)}), 400


if __name__ == "__main__":
    app.run()
