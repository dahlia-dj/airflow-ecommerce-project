from datetime import datetime, timedelta
import json
import logging
import os

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException


# Configuration generale

DATA_DIR = os.environ.get("ECOMMERCE_DATA_DIR", "/opt/airflow/data")
SOURCE_FILE = os.path.join(DATA_DIR, "dataset.csv")
ERROR_FILE = os.path.join(DATA_DIR, "errors.csv")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = "ecommerce_analytics"
MONGO_COLLECTION = "sales_metrics"

CATEGORIES_CONNUES = [
    "Computers Accessories",
    "Telephony",
    "Electronics",
    "Consoles Games",
    "Audio",
    "Fixed Telephony",
    "Computers",
    "Tablets Printing Image",
]


def slugify_categorie(categorie: str) -> str:
    """Transforme un libelle de categorie en identifiant de tache Airflow valide."""
    return categorie.lower().replace(" ", "_")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# Fonctions Python utilisées par les tâches

def check_file_exists(**context):
    """Etape 2 : verifie que le fichier source existe reellement."""
    if not os.path.isfile(SOURCE_FILE):
        raise FileNotFoundError(f"Fichier introuvable : {SOURCE_FILE}")
    logger.info("Fichier trouve : %s", SOURCE_FILE)
    return True


def check_file_not_empty(**context):
    """Etape 3 : verifie que le fichier n'est pas vide (0 octet ou 0 ligne de donnees)."""
    size = os.path.getsize(SOURCE_FILE)
    if size == 0:
        raise ValueError("Le fichier source est vide (0 octet.")

    df = pd.read_csv(SOURCE_FILE)
    if df.shape[0] == 0:
        raise ValueError("Le fichier source ne contient aucune ligne de donnees.")

    logger.info("Fichier non vide : %s lignes detectees", df.shape[0])
    context["ti"].xcom_push(key="nb_lignes_brutes", value=int(df.shape[0]))
    return True


def _line_key(df):
    """Retourne la cle d'unicite d'une ligne de vente."""
    return "IDLigne" if "IDLigne" in df.columns else "IDCommande"

#Etape 4 : controle qualité des données
def data_quality_check(**context):
    df = pd.read_csv(SOURCE_FILE)
    key_col = _line_key(df)

    errors = pd.DataFrame(columns=list(df.columns) + ["motif_rejet"])
    error_frames = []

    # Regle 1 : montant negatif
    montant_negatif = df[df["Montant"] < 0].copy()
    montant_negatif["motif_rejet"] = "montant_negatif"
    error_frames.append(montant_negatif)

    # Regle 2 : quantite nulle ou negative
    quantite_invalide = df[df["Quantite"] <= 0].copy()
    quantite_invalide["motif_rejet"] = "quantite_invalide"
    error_frames.append(quantite_invalide)

    # Regle 3 : ligne de vente dupliquee (meme cle d'unicite de ligne)
    doublons = df[df.duplicated(subset=[key_col], keep="first")].copy()
    doublons["motif_rejet"] = "ligne_dupliquee"
    error_frames.append(doublons)

    if error_frames:
        errors = pd.concat(error_frames, ignore_index=True)
        errors = errors.drop_duplicates(subset=[key_col, "motif_rejet"])

    invalid_keys = set(errors[key_col]) if not errors.empty else set()
    df_valid = df[~df[key_col].isin(invalid_keys)].copy()

    os.makedirs(os.path.dirname(ERROR_FILE), exist_ok=True)
    errors.to_csv(ERROR_FILE, index=False)

    nb_valid = df_valid.shape[0]
    nb_invalid = len(invalid_keys)
    nb_total = df.shape[0]

    logger.info(
        "Controle qualite : %s lignes valides, %s lignes rejetees sur %s (cle=%s)",
        nb_valid, nb_invalid, nb_total, key_col,
    )

    ti = context["ti"]
    ti.xcom_push(key="nb_lignes_valides", value=int(nb_valid))
    ti.xcom_push(key="nb_lignes_rejetees", value=int(nb_invalid))
    ti.xcom_push(key="error_file", value=ERROR_FILE)

    # Determination du statut pour le branchement
    if nb_valid == 0:
        return "quality_status_failed"
    elif nb_invalid > 0:
        return "quality_status_partial"
    else:
        return "quality_status_success"

#Etape 6 : charge uniquement les lignes valides
def load_data(**context):
    
    df = pd.read_csv(SOURCE_FILE)
    key_col = _line_key(df)
    errors = pd.read_csv(ERROR_FILE) if os.path.getsize(ERROR_FILE) > 0 else pd.DataFrame(columns=df.columns)
    invalid_keys = set(errors[key_col]) if not errors.empty else set()
    df_valid = df[~df[key_col].isin(invalid_keys)].copy()

    tmp_path = os.path.join(DATA_DIR, "dataset_valid.parquet")
    df_valid.to_parquet(tmp_path, index=False)

    context["ti"].xcom_push(key="valid_data_path", value=tmp_path)
    logger.info("Chargement termine : %s lignes valides ecrites dans %s", df_valid.shape[0], tmp_path)

 
#Etape 7 + 8 : calcule les indicateurs metier globaux et les transmet via XCom :
def compute_kpis(**context):
   
    ti = context["ti"]
    valid_data_path = ti.xcom_pull(task_ids="load_data", key="valid_data_path")
    df = pd.read_parquet(valid_data_path)
    df["Date"] = pd.to_datetime(df["Date"])
    df["Mois"] = df["Date"].dt.to_period("M").astype(str)

    #nombre total de commandes
    nb_commandes = df["IDCommande"].nunique()
    
    #nombre total de clients
    nb_clients = df["Client"].nunique()
    
    #chiffre d'affaires total
    chiffre_affaires = round(float(df["Montant"].sum()), 2)
    
    #panier moyen
    panier_moyen = round(chiffre_affaires / nb_commandes, 2) if nb_commandes else 0.0

    #top 10 des produits les plus vendus
    top_produits = (
        df.groupby("Produit")
        .agg(sales=("Quantite", "sum"), revenue=("Montant", "sum"))
        .sort_values("revenue", ascending=False)
        .head(10)
        .reset_index()
    )
    top_produits["revenue"] = top_produits["revenue"].round(2)
    top_produits_list = top_produits.to_dict(orient="records")

    #chiffre d'affaires par region
    ca_par_region = (
        df.groupby("Region")
        .agg(orders=("IDCommande", "nunique"), revenue=("Montant", "sum"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    ca_par_region["revenue"] = ca_par_region["revenue"].round(2)
    region_metrics = ca_par_region.rename(columns={"Region": "region"}).to_dict(orient="records")

    #evolution des ventes par mois
    evolution_mensuelle = (
        df.groupby("Mois")["Montant"].sum().round(2).reset_index()
        .rename(columns={"Montant": "chiffre_affaires"})
        .to_dict(orient="records")
    )

    global_metrics = {
        "nb_commandes": int(nb_commandes),
        "nb_clients": int(nb_clients),
        "chiffre_affaires": chiffre_affaires,
        "panier_moyen": panier_moyen,
    }

    ti.xcom_push(key="global_metrics", value=global_metrics)
    ti.xcom_push(key="top_products", value=top_produits_list)
    ti.xcom_push(key="region_metrics", value=region_metrics)
    ti.xcom_push(key="evolution_mensuelle", value=evolution_mensuelle)

    logger.info("KPI globaux calcules : %s", global_metrics)

 
#Etape 9 : tache generée dynamiquement (une par categorie de produit).
def analyse_categorie(categorie, **context):
   
    ti = context["ti"]
    valid_data_path = ti.xcom_pull(task_ids="load_data", key="valid_data_path")
    df = pd.read_parquet(valid_data_path)
    
    #Calcule le chiffre d'affaires et le volume vendu pour la categorie donnée.
    df_cat = df[df["Categorie"] == categorie]
    if df_cat.empty:
        logger.warning("Aucune donnee pour la categorie %s", categorie)
        raise AirflowSkipException(f"Pas de donnees pour {categorie}")

    resultat = {
        "categorie": categorie,
        "chiffre_affaires": round(float(df_cat["Montant"].sum()), 2),
        "quantite_vendue": int(df_cat["Quantite"].sum()),
        "nb_commandes": int(df_cat["IDCommande"].nunique()),
    }
    ti.xcom_push(key=f"metrics_{slugify_categorie(categorie)}", value=resultat)
    logger.info("Analyse categorie %s : %s", categorie, resultat)
    return resultat

#Recupere les resultats de toutes les taches dynamiques par categorie et les fusionne.
def aggregate_category_metrics(**context):
    
    ti = context["ti"]
    resultats = []
    for cat in CATEGORIES_CONNUES:
        val = ti.xcom_pull(task_ids=f"analyse_categorie_{slugify_categorie(cat)}", key=f"metrics_{slugify_categorie(cat)}")
        if val:
            resultats.append(val)
    ti.xcom_push(key="category_metrics", value=resultats)
    logger.info("Agregation des metriques par categorie : %s categories traitees", len(resultats))


#Etape 11 : genere un rapport final (JSON) qui recapitule l'execution du DAG.
def generate_final_report(**context):
    ti = context["ti"]
    execution_date = context["ds"]

    nb_valides = ti.xcom_pull(task_ids="data_quality_check", key="nb_lignes_valides") or 0
    nb_rejetees = ti.xcom_pull(task_ids="data_quality_check", key="nb_lignes_rejetees") or 0
    global_metrics = ti.xcom_pull(task_ids="compute_kpis", key="global_metrics") or {}
    top_products = ti.xcom_pull(task_ids="compute_kpis", key="top_products") or []
    region_metrics = ti.xcom_pull(task_ids="compute_kpis", key="region_metrics") or []
    evolution_mensuelle = ti.xcom_pull(task_ids="compute_kpis", key="evolution_mensuelle") or []
    category_metrics = ti.xcom_pull(task_ids="aggregate_category_metrics", key="category_metrics") or []

    if nb_valides == 0:
        status = "failed"
    elif nb_rejetees > 0:
        status = "partial"
    else:
        status = "success"

    rapport = {
        "execution_date": execution_date,
        "dag_id": "ecommerce_sales_pipeline",
        "dataset": "ecommerce_informatique",
        "source_file": os.path.basename(SOURCE_FILE),
        "status": status,
        "global_metrics": global_metrics,
        "top_products": top_products,
        "region_metrics": region_metrics,
        "evolution_mensuelle": evolution_mensuelle,
        "category_metrics": category_metrics,
        "quality": {
            "valid_rows": nb_valides,
            "invalid_rows": nb_rejetees,
            "error_file": os.path.basename(ERROR_FILE),
        },
    }

    report_path = os.path.join(DATA_DIR, f"rapport_{execution_date}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, indent=2, ensure_ascii=False)

    ti.xcom_push(key="rapport_final", value=rapport)
    logger.info("Rapport final genere : %s (statut=%s)", report_path, status)
    return rapport

#Etape 12 : stocke le document de metriques dans MongoDB.
def store_metrics_mongodb(**context):
    from pymongo import MongoClient

    ti = context["ti"]
    rapport = ti.xcom_pull(task_ids="generate_final_report", key="rapport_final")
    if not rapport:
        raise ValueError("Aucun rapport disponible a stocker dans MongoDB.")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    result = collection.insert_one(rapport)
    logger.info("Document insere dans MongoDB avec _id=%s", result.inserted_id)
    client.close()



# Definition du DAG


with DAG(
    dag_id="ecommerce_sales_pipeline",
    description="Pipeline d'industrialisation des ventes e-commerce (produits informatiques)",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "data-engineering", "mongodb"],
) as dag:

    # 1. Attente du fichier CSV
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=SOURCE_FILE,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 10,
        mode="poke",
    )

    # 2. Verification d'existence
    verify_file_exists = PythonOperator(
        task_id="verify_file_exists",
        python_callable=check_file_exists,
    )

    # 3. Verification que le fichier n'est pas vide
    verify_file_not_empty = PythonOperator(
        task_id="verify_file_not_empty",
        python_callable=check_file_not_empty,
    )

    # 4. Controle qualite des donnees (retourne le task_id a suivre)
    data_quality_check_task = BranchPythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    # 5. Branches possibles selon le controle qualite
    quality_status_success = EmptyOperator(task_id="quality_status_success")
    quality_status_partial = EmptyOperator(task_id="quality_status_partial")
    quality_status_failed = EmptyOperator(
        task_id="quality_status_failed",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Point de jonction : on continue le pipeline si succes OU partiel
    join_after_quality = EmptyOperator(
        task_id="join_after_quality",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 6. Chargement des donnees valides
    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    # 7-8. Calcul des KPI + XComs
    compute_kpis_task = PythonOperator(
        task_id="compute_kpis",
        python_callable=compute_kpis,
    )

    # 9. Creation dynamique des taches d'analyse par categorie
    category_tasks = []
    for categorie in CATEGORIES_CONNUES:
        t = PythonOperator(
            task_id=f"analyse_categorie_{slugify_categorie(categorie)}",
            python_callable=analyse_categorie,
            op_kwargs={"categorie": categorie},
        )
        category_tasks.append(t)

    aggregate_categories = PythonOperator(
        task_id="aggregate_category_metrics",
        python_callable=aggregate_category_metrics,
        trigger_rule=TriggerRule.ALL_DONE,  # continue meme si une categorie est vide/skip
    )

    # 11. Generation du rapport final (s'execute meme en cas d'echec partiel en amont)
    generate_report_task = PythonOperator(
        task_id="generate_final_report",
        python_callable=generate_final_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # 12. Stockage MongoDB (seulement si le rapport a pu etre genere)
    store_mongodb_task = PythonOperator(
        task_id="store_metrics_mongodb",
        python_callable=store_metrics_mongodb,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    
    # Orchestration (dependances)
    
    wait_for_file >> verify_file_exists >> verify_file_not_empty >> data_quality_check_task

    data_quality_check_task >> [quality_status_success, quality_status_partial, quality_status_failed]

    quality_status_success >> join_after_quality
    quality_status_partial >> join_after_quality

    join_after_quality >> load_data_task >> compute_kpis_task

    for t in category_tasks:
        compute_kpis_task >> t >> aggregate_categories

    aggregate_categories >> generate_report_task
    quality_status_failed >> generate_report_task

    generate_report_task >> store_mongodb_task
