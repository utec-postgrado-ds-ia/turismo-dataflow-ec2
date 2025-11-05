import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

s3 = boto3.client("s3")
BUCKET_NAME = "turismo-datalake-31102025"
PREFIX = "silver/lugar_turistico/"

def lambda_handler(event, context):
    key = event["Records"][0]["s3"]["object"]["key"]

    if not key.startswith(PREFIX):
        return {"status": "ignored"}

    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    contents = response.get("Contents", [])

    silver_objs = [obj for obj in contents if obj["Key"].endswith(".csv")]

    if not silver_objs:
        return {"status": "pending"}

    latest_silver = max(silver_objs, key=lambda x: x["LastModified"])
    silver_obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_silver["Key"])
    lugar_df = pd.read_csv(silver_obj["Body"])

    for col in ["nombre_ruta", "numero_lugares", "recomendado"]:
        if col in lugar_df.columns:
            lugar_df = lugar_df.drop(columns=[col])

    for date_col in ["fecha_opinion", "fecha_extraccion"]:
        if date_col in lugar_df.columns:
            lugar_df[date_col] = pd.to_datetime(lugar_df[date_col], errors="coerce")

    def calif_categoria(x):
        if pd.isna(x):
            return "Sin calificación"
        if x < 3:
            return "Baja"
        elif x < 4:
            return "Media"
        else:
            return "Alta"

    if "calificacion" in lugar_df.columns:
        lugar_df["calificacion_categoria"] = lugar_df["calificacion"].apply(calif_categoria)

    if "actividades" in lugar_df.columns:
        lugar_df["actividades_lista"] = lugar_df["actividades"].fillna("").apply(
            lambda x: [a.strip() for a in x.split(",")] if x else []
        )

    for coord in ["lat", "lng"]:
        if coord in lugar_df.columns:
            lugar_df[coord] = pd.to_numeric(lugar_df[coord], errors="coerce")

    for url_col in ["url", "imagen_url"]:
        if url_col in lugar_df.columns:
            lugar_df[url_col] = lugar_df[url_col].fillna("missing")

    if "descripcion" in lugar_df.columns:
        lugar_df["longitud_descripcion"] = lugar_df["descripcion"].fillna("").apply(len)

    if "actividades_lista" in lugar_df.columns:
        lugar_df["n_actividades"] = lugar_df["actividades_lista"].apply(len)

    for col in ["ruta_turistica", "region", "descripcion", "actividades"]:
        if col in lugar_df.columns:
            lugar_df[col] = lugar_df[col].fillna("missing")

    codigos_lugares = {
        "Museo de la Municipalidad de Quilca": "QML-981",
        "Caletas de Quilca": "QCL-472",
        "Playas de Quilca": "QPL-356",
        "Petroglifos de Quilca": "QPT-823",
        "Museo Arqueológico Instituto Superior Pedagógico La Inmaculada": "MIA-157",
        "Playas de Samuel Pastor - La Pampa": "SPP-689",
        "Parque Acuático de Camaná": "PAC-234",
        "Conjuntos arqueológicos Monte Pucor, Pillistay y Huamboy": "CAP-998",
        "Mirador del Inca": "MIN-441",
        "Bodeguillas de Huacapuy": "BHU-710",
        "Caleta del Inca": "CIN-526",
        "Bodega de Pisco Camaná – Ruta del Pisco (norte)": "BPC-812",
        "Complejo termal de la Juventud": "CTJ-301",
        "Complejo Termal de la Mamahuarmi": "CTM-479",
        "Baños Termales de Fierro": "BTF-633",
        "Baños termales de Tingo": "BTT-958",
        "Baños termales de Huancahuasi": "BTH-785",
        "Baños Termales de Picoy": "BTP-472",
        "Lomas de Lúcumo": "LLU-211",
        "Santuario Arqueológico de Pachacamac": "SAP-888",
        "Museo de Sitio de Pachacamac": "MSP-119",
        "Isla Cavillaca": "ICV-374",
        "Balneario de las Salinas": "BLS-540",
        "Santa Cruz de Flores": "SCF-267",
        "San Vicente de Azpitia": "SVA-651",
        "Santa Rosa de Quives": "SRQ-438",
        "Obrajillo": "OBR-903",
        "Cascada de Lucle": "CLL-125",
        "Sitio arqueológico Cantamarca": "SAC-712",
        "Bosque de Jarapampa": "BJR-485",
        "Lagunas Chuchún y Leoncocha": "LCL-329",
        "Cordillera La Viuda": "CLV-774",
        "Playa Lobitos": "PLB-602",
        "Playa Cabo Blanco": "PCB-158",
        "El Ñuro": "ENR-947",
        "Playa Punta Veleros": "PPV-386",
        "Los Órganos": "LOR-831",
        "Playa Vichayito": "PVH-224",
        "Playa Las Pocitas": "PLP-515",
        "Playa Máncora": "PLM-902",
        "Pilares de la Quebrada Fernández": "PQF-176",
        "Complejo Arqueológico de Miculla": "CAM-489",
        "Iglesia Nuestra Señora de las Mercedes": "INS-701",
        "Museo de sitio Las Peañas": "MSP-632",
        "Bodegas de pisco y vino artesanales": "BPV-349",
        "Iglesia Virgen del Rosario de Calana": "IVC-578",
        "Iglesia San José de Pachía": "ISP-864",
        "Campiña Tacneña": "CTC-291",
        "Catedral de Tacna": "CDT-933",
        "Fuente Ornamental": "FOR-142",
        "Arco Parabólico": "ARP-615",
        "Museo Histórico": "MHI-487",
        "Centro Cultural Casa Basadre": "CCB-712",
        "Casa De La Jurídica": "CDJ-356",
        "Casa de Francisco Antonio de Zela": "CFZ-289",
        "Teatro Municipal": "TMU-674",
        "Teatro Orfeón": "TOR-551",
        "Complejo Monumental del Campo de la Alianza": "CMA-803",
        "Humedales de Ite": "HIT-247",
        "Museo del desierto y el mar de Ite": "MDI-938",
        "Complejo Recreacional de Ite": "CRI-362",
        "Morro Sama": "MSA-415",
        "Playa Boca del Río": "PBR-791",
        "Mirador natural La Apacheta": "MLA-124",
        "Iglesia de Tarucachi": "ITA-568",
        "Iglesia San Benedicto": "ISB-309",
        "Iglesia de Ticaco": "ITC-874",
        "Andenería de Tarata": "ADT-446",
        "Puente Chacawira": "PCH-253",
        "Centro Arqueológico de Santa María": "CAS-682",
        "Centro de Interpretación (Municipal)": "CIM-517",
        "Cuevas de Qala Qala": "CQQ-391",
        "Baños Termales de Ticaco": "BTT-718",
        "Complejo Arqueológico de Tipón": "CAT-832",
        "Pikillaqta": "PIK-464",
        "Humedal de Huasao": "HHU-295",
        "Poblados de Saylla y Oropesa": "PSO-861",
        "Parque Arqueológico de Raqchi": "PAR-522",
        "Templo de San Pedro Apóstol de Andahuaylillas": "TSA-739",
        "Templo San Juan Bautista de Huaro": "TSJ-381",
        "Templo Virgen Purificada de Canincunca": "TVP-647",
        "Iglesia Matriz": "IMT-254",
        "Casa de Fierro": "CFI-913",
        "Ex Hotel Palace": "EHP-475",
        "Casa Fitzcarrald": "CFZ-591",
        "Casa Morey": "CMY-368",
        "Malecón Tarapacá": "MTA-820",
        "Museo Barco Histórico Ayapua 1906": "MBA-146",
        "Museo de culturas indígenas amazónicas": "MCI-582",
        "Centro cultural Irapay": "CCI-407",
        "Museo Iquitos": "MIQ-693",
        "Casona de Mercado Belén y Ex Pasaje Paquito": "CMB-239",
        "Mercado artesanal de San Juan": "MAS-317",
        "Playa Tipishca": "PTI-468",
        "Pueblo Tradicional de Santo Tomás": "PST-564",
        "Centro de Rescate Amazónico - CREA": "CRA-831",
        "Complejo Turístico de Quistococha": "CTQ-192",
        "Eco Park - Bosque de Huayo": "EPH-728",
        "Reserva Nacional Allpahuayo  Mishana": "RAM-350"
    }

    lugar_df["codigo_lugar"] = lugar_df["lugar_visitado"].map(codigos_lugares).fillna("missing")

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
    gold_key = f"gold/lugar_turistico/lugar_turistico_{timestamp}.csv"
    latest_key = "gold/lugar_turistico/latest/lugar_turistico_latest.csv"

    s3.put_object(Bucket=BUCKET_NAME, Key=gold_key, Body=csv_buffer.getvalue())

    s3.copy_object(
        Bucket=BUCKET_NAME,
        CopySource={"Bucket": BUCKET_NAME, "Key": gold_key},
        Key=latest_key
    )

    return {
        "status": "ok",
        "rows": len(lugar_df),
        "input_silver": latest_silver["Key"],
        "output_gold": gold_key,
        "latest_path": latest_key
    }