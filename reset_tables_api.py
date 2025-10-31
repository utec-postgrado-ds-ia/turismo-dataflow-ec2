import random
from flask import Flask, jsonify, request
from datetime import datetime, timedelta
from faker import Faker
import psycopg2

app = Flask(__name__)

# Configura tus variables de conexión a RDS
DB_HOST = "turismo-db.cv8ueqky6eu4.us-east-1.rds.amazonaws.com"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "postgresql"

TABLAS = {
    "Opiniones_Turisticas": """
        CREATE TABLE Opiniones_Turisticas (
            opinion_id VARCHAR(50) PRIMARY KEY,
            lugar_visitado VARCHAR(100),
            usuario VARCHAR(50),
            calificacion REAL,
            comentario TEXT,
            fecha_opinion DATE,
            fuente VARCHAR(100)
        );
    """,
    "Ofertas_Turisticas_Competencia": """
        CREATE TABLE Ofertas_Turisticas_Competencia (
            oferta_id VARCHAR(50) PRIMARY KEY,
            proveedor VARCHAR(100),
            destino VARCHAR(100),
            nombre_paquete VARCHAR(150),
            duracion_dias INT,
            precio_usd REAL,
            fecha_inicio DATE,
            fecha_fin DATE,
            tipo_paquete VARCHAR(50),
            incluye_transporte BOOLEAN
        );
    """,
    "Reservas_Competencia": """
        CREATE TABLE Reservas_Competencia (
            reserva_id VARCHAR(50) PRIMARY KEY,
            proveedor VARCHAR(100),
            destino VARCHAR(100),
            cliente_tipo VARCHAR(50),
            fecha_reserva DATE,
            cantidad_personas INT,
            monto_total_usd REAL
        );
    """
}

LUGARES = [
    "Museo de la Municipalidad de Quilca",
    "Caletas de Quilca",
    "Playas de Quilca",
    "Petroglifos de Quilca",
    "Museo Arqueológico Instituto Superior Pedagógico La Inmaculada",
    "Playas de Samuel Pastor - La Pampa",
    "Parque Acuático de Camaná",
    "Conjuntos arqueológicos Monte Pucor, Pillistay y Huamboy",
    "Mirador del Inca",
    "Bodeguillas de Huacapuy",
    "Caleta del Inca",
    "Bodega de Pisco Camaná – Ruta del Pisco (norte)",
    "Complejo termal de la Juventud",
    "Complejo Termal de la Mamahuarmi",
    "Baños Termales de Fierro",
    "Baños termales de Tingo",
    "Baños termales de Huancahuasi",
    "Baños Termales de Picoy",
    "Lomas de Lúcumo",
    "Santuario Arqueológico de Pachacamac",
    "Museo de Sitio de Pachacamac",
    "Isla Cavillaca",
    "Balneario de las Salinas",
    "Santa Cruz de Flores",
    "San Vicente de Azpitia",
    "Santa Rosa de Quives",
    "Obrajillo",
    "Cascada de Lucle",
    "Sitio arqueológico Cantamarca",
    "Bosque de Jarapampa",
    "Lagunas Chuchún y Leoncocha",
    "Cordillera La Viuda",
    "Playa Lobitos",
    "Playa Cabo Blanco",
    "El Ñuro",
    "Playa Punta Veleros",
    "Los Órganos",
    "Playa Vichayito",
    "Playa Las Pocitas",
    "Playa Máncora",
    "Pilares de la Quebrada Fernández",
    "Complejo Arqueológico de Miculla",
    "Iglesia Nuestra Señora de las Mercedes",
    "Museo de sitio Las Peañas",
    "Bodegas de pisco y vino artesanales",
    "Iglesia Virgen del Rosario de Calana",
    "Iglesia San José de Pachía",
    "Campiña Tacneña",
    "Catedral de Tacna",
    "Fuente Ornamental",
    "Arco Parabólico",
    "Museo Histórico",
    "Centro Cultural Casa Basadre",
    "Casa De La Jurídica",
    "Casa de Francisco Antonio de Zela",
    "Teatro Municipal",
    "Teatro Orfeón",
    "Complejo Monumental del Campo de la Alianza",
    "Humedales de Ite",
    "Museo del desierto y el mar de Ite",
    "Complejo Recreacional de Ite",
    "Morro Sama",
    "Playa Boca del Río",
    "Mirador natural La Apacheta",
    "Iglesia de Tarucachi",
    "Iglesia San Benedicto",
    "Iglesia de Ticaco",
    "Andenería de Tarata",
    "Puente Chacawira",
    "Centro Arqueológico de Santa María",
    "Centro de Interpretación (Municipal)",
    "Cuevas de Qala Qala",
    "Baños Termales de Ticaco",
    "Complejo Arqueológico de Tipón",
    "Pikillaqta",
    "Humedal de Huasao",
    "Poblados de Saylla y Oropesa",
    "Parque Arqueológico de Raqchi",
    "Templo de San Pedro Apóstol de Andahuaylillas",
    "Templo San Juan Bautista de Huaro",
    "Templo Virgen Purificada de Canincunca",
    "Iglesia Matriz",
    "Casa de Fierro",
    "Ex Hotel Palace",
    "Casa Fitzcarrald",
    "Casa Morey",
    "Malecón Tarapacá",
    "Museo Barco Histórico Ayapua 1906",
    "Museo de culturas indígenas amazónicas",
    "Centro cultural Irapay",
    "Museo Iquitos",
    "Casona de Mercado Belén y Ex Pasaje Paquito",
    "Mercado artesanal de San Juan",
    "Playa Tipishca",
    "Pueblo Tradicional de Santo Tomás",
    "Centro de Rescate Amazónico - CREA",
    "Complejo Turístico de Quistococha",
    "Eco Park - Bosque de Huayo",
    "Reserva Nacional Allpahuayo  Mishana"
]

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

@app.route("/reset-tables", methods=["POST"])
def reset_tables():
    try:
        conn = get_conn()
        cur = conn.cursor()

        for tabla, create_sql in TABLAS.items():
            cur.execute(f"DROP TABLE IF EXISTS {tabla} CASCADE;")
            cur.execute(create_sql)

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "ok", "message": "Tablas creadas correctamente."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/<tabla>", methods=["GET"])
def consultar_tabla(tabla):
    if tabla not in TABLAS:
        return jsonify({"status": "error", "message": f"Tabla {tabla} no encontrada."}), 404

    try:
        conn = get_conn()
        cur = conn.cursor()

        query = f"SELECT * FROM {tabla}"

        filtros = []
        valores = []
        for key, value in request.args.items():
            filtros.append(f"{key} = %s")
            valores.append(value)

        if filtros:
            query += " WHERE " + " AND ".join(filtros)

        query += " LIMIT 100;"

        cur.execute(query, valores)
        columnas = [desc[0] for desc in cur.description]
        filas = cur.fetchall()
        resultados = [dict(zip(columnas, fila)) for fila in filas]

        cur.close()
        conn.close()
        return jsonify({"status": "ok", "data": resultados})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/generar-opiniones/<int:cantidad>", methods=["POST"])
def generar_opiniones(cantidad):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        # Borrar datos existentes
        cur.execute("DELETE FROM Opiniones_Turisticas;")

        for _ in range(cantidad):
            opinion_id = faker.uuid4()
            lugar_visitado = random.choice(LUGARES)
            usuario = faker.user_name()
            calificacion = round(random.uniform(1.0, 5.0), 1)
            comentario = faker.sentence(nb_words=20)
            fecha_opinion = faker.date_between(start_date='-2y', end_date='today')
            fuente = random.choice(["Web", "Encuesta", "Manual"])

            cur.execute("""
                INSERT INTO Opiniones_Turisticas 
                (opinion_id, lugar_visitado, usuario, calificacion, comentario, fecha_opinion, fuente)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (opinion_id, lugar_visitado, usuario, calificacion, comentario, fecha_opinion, fuente))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok", "message": f"{cantidad} opiniones generadas correctamente."})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)