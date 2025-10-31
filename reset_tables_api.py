from flask import Flask, jsonify
import psycopg2
import os

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

@app.route("/reset-tables", methods=["POST"])
def reset_tables():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        for tabla, create_sql in TABLAS.items():
            # Borra tabla si existe
            cur.execute(f"DROP TABLE IF EXISTS {tabla} CASCADE;")
            # Crea tabla
            cur.execute(create_sql)

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok", "message": "Tablas creadas correctamente."})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":

    app.run(host="0.0.0.0", port=5001)
