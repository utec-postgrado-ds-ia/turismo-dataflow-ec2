import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

s3 = boto3.client("s3")
BUCKET_NAME = "turismo-datalake-31102025"
PREFIX = "raw/lugar_turistico/"

def lambda_handler(event, context):
    key = event["Records"][0]["s3"]["object"]["key"]

    if not key.startswith(PREFIX):
        return {"status": "ignored"}

    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    contents = response.get("Contents", [])

    rutas_objs = [obj for obj in contents if "rutas_turisticas_destacadas_" in obj["Key"]]
    opiniones_objs = [obj for obj in contents if "opiniones_turisticas_" in obj["Key"]]

    if not rutas_objs or not opiniones_objs:
        return {"status": "pending"}

    latest_rutas = max(rutas_objs, key=lambda x: x["LastModified"])
    latest_opiniones = max(opiniones_objs, key=lambda x: x["LastModified"])

    rutas_obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_rutas["Key"])
    opiniones_obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_opiniones["Key"])

    rutas_df = pd.read_csv(rutas_obj["Body"])
    opiniones_df = pd.read_csv(opiniones_obj["Body"])

    if "subsitio_nombre" not in rutas_df.columns or "lugar_visitado" not in opiniones_df.columns:
        return {"status": "error", "message": "Faltan columnas para merge"}

    lugar_df = pd.merge(
        opiniones_df,
        rutas_df,
        left_on="lugar_visitado",
        right_on="subsitio_nombre",
        how="inner"
    )

    for col in ["nombre_ruta", "numero_lugares", "recomendado"]:
        if col in lugar_df.columns:
            lugar_df = lugar_df.drop(columns=[col])

    for col in lugar_df.select_dtypes(include=["object"]).columns:
        lugar_df[col] = (
            lugar_df[col]
            .astype(str)
            .str.replace(r"[\r\n]+", " ", regex=True)
            .str.strip()
        )

    csv_buffer = StringIO()
    lugar_df.to_csv(csv_buffer, index=False)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    silver_key = f"silver/lugar_turistico/lugar_turistico_{timestamp}.csv"
    s3.put_object(Bucket=BUCKET_NAME, Key=silver_key, Body=csv_buffer.getvalue())

    return {
        "status": "ok",
        "rows": len(lugar_df),
        "lugares_usado": latest_rutas["Key"],
        "opiniones_usada": latest_opiniones["Key"],
        "output": silver_key
    }