#!/usr/bin/python3
import os
from logging.config import dictConfig


from flask import Flask, jsonify, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import psycopg
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool

from random import random

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

app = Flask(__name__)
app.config.from_prefixed_env()
app.json.sort_keys = False  # Preserve insertion order
app.json.ensure_ascii = False
log = app.logger

limiter = Limiter(
  key_func=get_remote_address,
  app=app,
  default_limits=["500 per day", "200 per hour"]
)

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


def generate_price(first_class: bool) -> float:
    if first_class:
        result = 500 + (random() * 1000)
    else:
        result = 150 + (random() * 400)

    return round(result, 2)


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

                return jsonify([{
                    "nome": airport.nome,
                    "cidade": airport.cidade,
                } for airport in airports]), 200

            except Exception as e:
                return jsonify({"error:": str(e)}), 500


@app.route("/voos/<partida>/", methods=("GET", ))
def airport_departures(partida):
    """
    Returns all flights (plane serial number, hour of departure and airport of destination)
    that departure from <partida> up until after 12h.
    """

    if not partida or len(partida) != 3 or not partida.isalpha():
        return jsonify({"status": "error", "message": "O código de um aeroporto consiste em três letras"}), 400

    partida = partida.upper()

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
                    return jsonify({"message": f"Airport '{partida}' not found", "status": "error"}), 404

                departures = cur.execute(
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

                return jsonify([{
                    "número de serie": departure.no_serie,
                    "hora de partida": departure.hora_partida,
                    "aeroporto de chegada": departure.chegada
                } for departure in departures]
                ), 200
            except Exception as e:
                return jsonify({"error:": str(e)}), 500


@app.route("/voos/<partida>/<chegada>/", methods=("GET",))
def available_flights(partida, chegada):
    """
    Returns the next 3 flights (plane serial number, hour of departure and airport of destination),
    between <partida> and <chegada> airports, that have available tickets.
    """

    if not (
        len(partida) == 3 and len(chegada) == 3 or
        partida.isalpha() and chegada.isalpha() or
        partida.isupper() and chegada.isupper()
    ):
        return jsonify({"status": "error", "message": "O código de um aeroporto consiste em três letras"}), 400

    if partida == chegada:
        return jsonify({"status": "error", "message": "Os aeroportos de chegada e partida não podem ser iguais"}), 400

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
                    return jsonify({"status": "error", "message": f"Airport '{partida}' not found"}), 404

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
                    return jsonify({"status": "error", "message": f"Airport '{chegada}' not found"}), 404

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

                return jsonify([{
                    "número de serie": flight.no_serie,
                    "hora de partida": flight.hora_partida,
                } for flight in flights]
                ), 200

            except Exception as e:
                return jsonify({"error": str(e)}), 400


@app.route("/compra/<voo>/", methods=("POST",))
def purchase_ticket(voo):
    """
    Purchase tickets for a flight.
    Expects JSON body:
    {
        "nif_cliente": "123456789",
        "bilhetes": [
            {"nome": "Jane Doe", "classe": "primeira"},
            {"nome": "John Doe", "classe": "economica"}
        ]
    }
    """

    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "JSON com os dados é obrigatório."}), 400

    nif_client = data.get("nif_cliente")
    tickets = data.get("bilhetes")

    error = None

    if not nif_client:
        error = "Parameter 'nif_cliente' is necessary."
    elif len(nif_client) != 9 or not nif_client.isdigit():
        error = "NIF must have exactly 9 digits."

    if not error and not tickets:
        error = "Parameter 'bilhetes' is necessary: list of pairs of (passenger_name, class ('economica'| 'primeira')). Example: bilhetes=John Doe,economica;Jane Smith,primeira;Bob Wilson,economica"

    tickets_list = []
    if not error:
        try:
            for i, ticket in enumerate(tickets):
                if not isinstance(ticket, dict):
                    error = f"Bilhete {i+1}: Deve ser um objeto."
                    break

                name = ticket.get('nome', '').strip()
                classe = ticket.get('classe', '').lower()

                # Validate passenger name
                if len(name) > 80:
                    error = f"Bilhete {i+1}: Nome do passageiro é muito longo."
                    break

                if classe not in ("economica", "primeira"):
                    error = f"Bilhete {i+1}: Classe deve ser 'economica' ou 'primeira'."
                    break

                tickets_list.append({"name": name, "class": classe})

        except Exception:
            error = "Formato inválido do parâmetro 'bilhetes'. Use: 'Nome1, Classe1; Nome2,Classe2'"

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
                        SELECT v.id, v.no_serie, v.hora_partida, v.partida, v.chegada 
                        FROM voo v
                        WHERE v.id = %(voo_id)s AND v.hora_partida > NOW();
                        """, {"voo_id": voo}
                    ).fetchone()

                    if not flight:
                        return jsonify({
                            "message": f"Voo {voo} não encontrado ou já partiu.",
                            "status": "error"
                        }), 404

                    # Create sale record
                    venda = cur.execute(
                        """
                        INSERT INTO venda (nif_cliente, balcao, hora)
                        VALUES (%(nif_cliente)s, %(balcao)s, NOW())
                        RETURNING codigo_reserva;
                        """,
                        {"nif_cliente": nif_client, "balcao": flight.partida}
                    ).fetchone().codigo_reserva

                    log.info(f"venda criada: {venda}")

                    created_tickets = []

                    # Generate price according to class
                    first_class_price = generate_price(True)
                    economy_class_price = generate_price(False)

                    for ticket in tickets_list:

                        log.info(f"ticket: {ticket}")

                        first_class = ticket["class"] == "primeira"

                        price = first_class_price if first_class else economy_class_price

                        id = cur.execute(
                            """
                            INSERT INTO bilhete (voo_id, codigo_reserva, nome_passegeiro, preco, prim_classe)
                            VALUES (%(voo_id)s, %(codigo_reserva)s, %(nome)s, %(preco)s, %(classe)s)
                            RETURNING id;
                            """,
                            {"voo_id": voo,
                             "codigo_reserva": venda,
                             "nome": ticket["name"],
                             "preco": price, "classe": first_class}
                        ).fetchone().id

                        log.info(f"tickedid: {id}")

                        ticket_dict = {
                            "id": id,
                            "name": ticket["name"],
                            "classe": ticket["class"],
                            "preco": price
                        }

                        log.info(ticket_dict)

                        created_tickets.append(ticket_dict)

                    # If we reach here, ALL operations succeeded
                    # COMMIT happens automatically at the end of the transaction block

            except psycopg.Error as e:
                log.error(f"Trigger error when inserting ticket: {e}")

                error_str = str(e)
                n_pos = error_str.find('\n')
                if n_pos != -1:
                    error_str = error_str[:n_pos]

                return jsonify({
                    "status": "error",
                    "message": "Erro ao processar compra. Nenhuma alteração foi feita.",
                    "erro": error_str
                }), 400

            except Exception as e:
                # ROLLBACK is automatic
                log.error(f"Transaction failed: {str(e)}")
                return jsonify({
                    "status": "error",
                    "message": "Erro ao processar compra. Nenhuma alteração foi feita.",
                    "erro": str(e)
                }), 500

            # COMMIT is executed at the end of the block.

    log.debug(f"created_tickets: {created_tickets}")
    return jsonify({
        "status": "success",
        "message": "Compra realizada com sucesso",
        "codigo_reserva": venda,
        "voo": {
            "id": flight.id,
            "no_serie": flight.no_serie,
            "hora_partida": flight.hora_partida,
            "aeroporto_partida": flight.partida,
            "aeroporto_chegada": flight.chegada
        },
        "bilhetes": created_tickets
    }), 201


@app.route("/checkin/<bilhete>/", methods=("POST",))
def checkin_ticket(bilhete):
    """
    Check-in a ticket and automatically assign a seat.
    """

    with pool.connection() as conn:
        with conn.cursor() as cur:

            try:
                with conn.transaction():

                    # Ticket details
                    ticket = cur.execute(
                        """
                        SELECT b.id, b.voo_id, b.nome_passegeiro, b.prim_classe, b.lugar, b.no_serie as ticket_no_serie, v.no_serie as flight_no_serie
                        FROM bilhete b
                        JOIN voo v ON b.voo_id = v.id
                        WHERE b.id = %(ticket)s
                        AND v.hora_partida > NOW();
                        """,
                        {"ticket": bilhete}
                    ).fetchone()

                    if not ticket:
                        return jsonify({
                            "status": "error",
                            "message": "Bilhete não encontrado",
                        }), 404

                    # Check if already checked in
                    if ticket.lugar and ticket.ticket_no_serie:
                        return jsonify({
                            "status": "success",
                            "message": "Check-in para este bilhete já foi realizado",
                            "assento": ticket.lugar,
                            "passageiro": ticket.nome_passegeiro
                        }), 200

                    available_seat = cur.execute(
                        """
                        SELECT a.lugar
                        FROM assento a
                        WHERE a.no_serie = %(no_serie)s
                        AND a.prim_classe = %(class)s
                        AND NOT EXISTS(
                           SELECT 1 FROM bilhete b2
                           WHERE b2.lugar = a.lugar
                           AND b2.no_serie = a.no_serie
                           AND b2.id != %(ticket_id)s
                        )
                        LIMIT 1;
                        """,
                        {"no_serie": ticket.flight_no_serie,
                            "class": ticket.prim_classe, "ticket_id": bilhete}
                    ).fetchone()
                    # NOTE: the first found seat is assigned (within the ticket class)

                    if not available_seat:
                        # NOTE: unreachable
                        return jsonify({
                            "status": "error",
                            "message": f"Não há assentos disponíveis "
                        }), 400

                    cur.execute(
                        """
                        UPDATE bilhete
                        SET lugar = %(lugar)s, no_serie = %(no_serie)s
                        WHERE id = %(id)s;
                        """,
                        {"lugar": available_seat.lugar,
                            "no_serie": ticket.flight_no_serie,
                            "id": bilhete}
                    )

            except Exception as e:
                log.error(
                    f"Internal error in checkin_ticket({bilhete}): {str(e)} ")
                return jsonify({
                    "status": "error",
                    "message": "Erro ao processar check-in."
                }), 500

    return jsonify({
        "status": "success",
        "message": "Check-in realizado com sucesso",
        "bilhete_id": bilhete,
        "passageiro": ticket.nome_passegeiro,
        "assento": available_seat.lugar,
        "classe": "primeira classe" if ticket.prim_classe else "classe económica"
    }), 201


if __name__ == "__main__":
    app.run()
