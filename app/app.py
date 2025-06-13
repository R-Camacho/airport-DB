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
                    SELECT nome, cidade
                    FROM aeroporto;
                    """,
                    {},
                ).fetchall()
                log.debug(f"{cur.rowcount} airports found.")

                return jsonify(airports), 200
            except Exception as e:
                return jsonify({"error:": str(e)}), 500


@app.route("/voos/<partida>/", methods=("GET", ))
def airport_departures(partida):
    """
    Returns all flights (plane serial number, hour of departure and airport of destination)
    that departure from <partida> up until after 12h.
    """

    # TODO: ver o que fazer se o user colocar em letra maiúscula.
    # Ex: se colocar fnc em vez de FNC

    # TODO: maybe dar erro se nao tiver 3 letras ou o erro de cima

    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                # Check if airport exists
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
                    return jsonify({"message": f"Airport '{partida}' not found", "status": "error"}), 404

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
                return jsonify({"error:": str(e)}), 500


@app.route("/voos/<partida>/<chegada>/", methods=("GET",))
def available_flights(partida, chegada):
    """
    Returns the next 3 flights (plane serial number, hour of departure and airport of destination),
    between <partida> and <chegada> airports, that have available tickets.
    """

    # TODO: tratamento de erros

    if partida == chegada:
        return jsonify({"message": "Departure and arrival airports can't be the same", "status": "error"}), 400

    if not (
        len(partida) == 3 and len(chegada) == 3 or
        partida.isalpha() and chegada.isalpha() or
        partida.isupper() and chegada.isupper()
    ):
        return jsonify({"message": "Airport codes consist of 3 upper case letters", "status": "error"}), 400

    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                # Check if departure airport exists
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
                    return jsonify({"message": f"Airport '{partida}' not found", "status": "error"}), 404

                # Check if arrivasl airport exists
                cur.execute(
                    """
                    SELECT *
                    FROM aeroporto
                    WHERE codigo = %(chegada)s;
                    """,
                    {"chegada": chegada},
                )
                if not cur.fetchone():
                    # TODO: acho que o erro é 404 (not found)
                    # TODO: confirmar todos os códigos de erro em
                    # https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status
                    return jsonify({"message": f"Airport '{chegada}' not found", "status": "error"}), 404

                flights = cur.execute(
                    """
                    SELECT DISTINCT v.no_serie, v.hora_partida
                    FROM voo v
                    JOIN assento a USING (no_serie)
                    LEFT JOIN bilhete b ON v.id = b.voo_id AND a.lugar = b.lugar AND a.no_serie = b.no_serie
                    WHERE v.partida = %(partida)s
                    AND v.chegada = %(chegada)s
                    AND v.hora_partida > NOW()
                    AND b.id IS NULL -- if no matching ticket exists         
                    ORDER BY v.hora_partida
                    LIMIT 3;
                    """,
                    {"partida": partida, "chegada": chegada}
                ).fetchall()

                log.debug(
                    f"{cur.rowcount} available flights found between {partida.upper()} and {chegada.upper()}")

                return jsonify(flights), 200

            except Exception as e:
                return jsonify({"error": str(e)}), 400


@app.route("/compra/<voo>", methods=("POST",))
def purchase_ticket(voo):
    """
    Purchase tickets for a flight.
    Query parameters:
    - nif_cliente: Customer NIF (9 digits)
    - bilhetes: List of pairs of (passenger_name, class), separated by semicolon, and values separated by comma
    Example: /compra/123/?nif_cliente=123456789&bilhetes=John Doe,economica;Jane Smith,primeira;Bob Wilson,economica
    """

    nif_client = request.args.get("nif_cliente")
    tickets = request.args.get("bilhetes")

    error = None

    if not nif_client:
        error = "Parameter 'nif_cliente' is necessary."
    elif len(nif_client) != 9 or not nif_client.isdigit():
        error = "NIF must have exactly 9 digits."

    if not error and not tickets:
        error = "Parameter 'bilhetes' is necessary: list of pairs of (passenger_name, class ('economica'| 'primeira'))\nExample: bilhetes=John Doe,economica;Jane Smith,primeira;Bob Wilson,economica"

    if not error:
        tickets_list = []
        try:
            # Separated by semicolon
            all_tickets = [b.strip() for b in tickets.split(';') if b.strip()]

            if not all_tickets:
                # TODO: ver tratamento deste erro
                error = "Parameter 'bilhetes' is necessary: list of pairs of (passenger_name, class ('economica'| 'primeira'))\nExample: bilhetes=John Doe,economica;Jane Smith,primeira;Bob Wilson,economica"
            else:
                for i, ticket in enumerate(all_tickets):
                    values = [p.strip() for p in ticket.split(',')]

                    if len(values) != 2:
                        error = f"Bilhete {i+1}: Formato deve ser 'Nome,Classe'."
                        break

                    name, classe = values

                    # Validate passenger name
                    if len(name) > 80:
                        error = f"Bilhete {i+1}: Nome do passageiro é muito longo."
                        break

                    classe = classe.lower()
                    if classe not in ("economica", "primeira"):
                        error = f"Bilhete {i+1}: Classe deve ser 'economica' ou 'primeira'."
                        break

                tickets_list.append({"name": name, "class": classe})

        except Exception:
            error = "Invalid format of parameter 'bilhetes'. Use: 'Nome1, Classe1; Nome2,Classe2'"

    if error is not None:
        return jsonify({"message": error, "status": "error"}), 400

    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                with conn.transaction():
                    # BEGIN is executed, a transaction started

                    # Check if flight exists and get details
                    flight = cur.execute(
                        """
                        SELECT v.id, v.no_serie, v.hora_partida, v.partida, a.nome as aeroporto_partida
                        FROM voo v
                        JOIN aeroporto a ON v.partida = a.codigo
                        WHERE v.id = %(voo)s AND v.hora_partida > NOW();
                        """, {"voo_id": voo}
                    ).fetchone()

                    if not flight:
                        return jsonify({
                            "message": f"Voo {voo} não encontrado ou já partiu.",
                            "status": "error"
                        }), 404

                    # TODO: Check capacity for each class


                    # TODO: Create bilhete

            except Exception as e:
                pass

            else:
                pass
                # COMMIT is executed at the end of the block.


if __name__ == "__main__":
    app.run()
