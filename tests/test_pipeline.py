"""
Tests unitaires du pipeline ecommerce_sales_pipeline.

Ces tests valident la logique metier independamment d'Airflow :
  - detection des fichiers manquants / vides ;
  - regles de gestion (montant negatif, quantite invalide, doublons) ;
  - calcul des indicateurs (KPI) ;
  - coherence du rapport final.

Execution : pytest tests/test_pipeline.py -v
"""

import os
import sys
import json
import tempfile
import shutil

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))


@pytest.fixture
def temp_data_dir(monkeypatch):
    """Cree un repertoire temporaire de donnees et patche les chemins du module DAG."""
    tmp_dir = tempfile.mkdtemp()
    monkeypatch.setenv("ECOMMERCE_DATA_DIR", tmp_dir)

    import importlib
    import ecommerce_sales_pipeline as pipeline
    importlib.reload(pipeline)

    pipeline.DATA_DIR = tmp_dir
    pipeline.SOURCE_FILE = os.path.join(tmp_dir, "dataset.csv")
    pipeline.ERROR_FILE = os.path.join(tmp_dir, "errors.csv")

    yield pipeline, tmp_dir
    shutil.rmtree(tmp_dir, ignore_errors=True)


class FakeTaskInstance:
    """Simule le comportement minimal d'un TaskInstance Airflow pour les XComs."""

    def __init__(self):
        self._store = {}

    def xcom_push(self, key, value):
        self._store[key] = value

    def xcom_pull(self, task_ids=None, key=None):
        return self._store.get(key)


def make_sample_csv(path, rows):
    df = pd.DataFrame(
        rows,
        columns=["Date", "IDCommande", "Produit", "Categorie", "Quantite", "Prix", "Montant", "Region", "Client"],
    )
    df.to_csv(path, index=False)
    return df


# Tests : verification de presence / non-vacuite du fichier

def test_check_file_exists_raises_when_missing(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    with pytest.raises(FileNotFoundError):
        pipeline.check_file_exists()


def test_check_file_exists_ok(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
    ])
    assert pipeline.check_file_exists() is True


def test_check_file_not_empty_raises_on_empty_file(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    open(pipeline.SOURCE_FILE, "w").close()
    ti = FakeTaskInstance()
    with pytest.raises(ValueError):
        pipeline.check_file_not_empty(ti=ti)


def test_check_file_not_empty_passes_with_data(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
    ])
    ti = FakeTaskInstance()
    assert pipeline.check_file_not_empty(ti=ti) is True
    assert ti.xcom_pull(key="nb_lignes_brutes") == 1


# Tests : regles de gestion metier / controle qualite

def test_data_quality_rejects_negative_amount(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, -1200, "Paris", "CUST1"],
        ["2026-01-01 10:05:00", "ORD2", "Souris Logitech", "Informatique", 2, 25, 50, "Lyon", "CUST2"],
    ])
    ti = FakeTaskInstance()
    branch = pipeline.data_quality_check(ti=ti)
    assert branch == "quality_status_partial"
    assert ti.xcom_pull(key="nb_lignes_rejetees") == 1
    assert ti.xcom_pull(key="nb_lignes_valides") == 1


def test_data_quality_rejects_zero_or_negative_quantity(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 0, 1200, 0, "Paris", "CUST1"],
        ["2026-01-01 10:05:00", "ORD2", "Souris Logitech", "Informatique", -2, 25, -50, "Lyon", "CUST2"],
        ["2026-01-01 10:10:00", "ORD3", "Casque Gamer", "Informatique", 1, 55, 55, "Lille", "CUST3"],
    ])
    ti = FakeTaskInstance()
    branch = pipeline.data_quality_check(ti=ti)
    assert branch == "quality_status_partial"
    assert ti.xcom_pull(key="nb_lignes_valides") == 1
    assert ti.xcom_pull(key="nb_lignes_rejetees") == 2


def test_data_quality_rejects_duplicate_order_id(temp_data_dir):
    # Comportement de repli : en l'absence de colonne IDLigne (ancien format
    # mono-ligne-par-commande), la cle d'unicite retombe sur IDCommande. Les
    # deux occurrences d'un IDCommande duplique sont alors exclues (on ne peut
    # pas determiner de facon fiable laquelle est la version correcte).
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
        ["2026-01-01 10:05:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
        ["2026-01-01 10:10:00", "ORD2", "Souris Logitech", "Informatique", 2, 25, 50, "Lyon", "CUST2"],
    ])
    ti = FakeTaskInstance()
    branch = pipeline.data_quality_check(ti=ti)
    assert branch == "quality_status_partial"
    assert ti.xcom_pull(key="nb_lignes_rejetees") == 1
    assert ti.xcom_pull(key="nb_lignes_valides") == 1


def test_data_quality_all_valid_returns_success(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
        ["2026-01-01 10:05:00", "ORD2", "Souris Logitech", "Informatique", 2, 25, 50, "Lyon", "CUST2"],
    ])
    ti = FakeTaskInstance()
    branch = pipeline.data_quality_check(ti=ti)
    assert branch == "quality_status_success"
    assert ti.xcom_pull(key="nb_lignes_rejetees") == 0


def test_data_quality_allows_multi_item_orders_with_idligne(temp_data_dir):
    """
    Sur des donnees reelles (Olist), une commande peut contenir plusieurs
    lignes de produits differents : IDCommande se repete alors legitimement.
    Quand la colonne IDLigne (cle unique de ligne) est presente, ce cas ne
    doit PAS etre traite comme un doublon.
    """
    pipeline, tmp_dir = temp_data_dir
    df = pd.DataFrame([
        ["2026-01-01 10:00:00", "ORD1", "ORD1_1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
        ["2026-01-01 10:00:00", "ORD1", "ORD1_2", "Souris Logitech", "Informatique", 1, 25, 25, "Paris", "CUST1"],
        ["2026-01-01 10:05:00", "ORD2", "ORD2_1", "Casque Gamer", "Informatique", 1, 55, 55, "Lyon", "CUST2"],
    ], columns=["Date", "IDCommande", "IDLigne", "Produit", "Categorie", "Quantite", "Prix", "Montant", "Region", "Client"])
    df.to_csv(pipeline.SOURCE_FILE, index=False)

    ti = FakeTaskInstance()
    branch = pipeline.data_quality_check(ti=ti)
    assert branch == "quality_status_success"
    assert ti.xcom_pull(key="nb_lignes_valides") == 3
    assert ti.xcom_pull(key="nb_lignes_rejetees") == 0


def test_data_quality_rejects_exact_duplicate_line_with_idligne(temp_data_dir):
    """Une ligne strictement dupliquee (meme IDLigne) doit, elle, etre rejetee."""
    pipeline, tmp_dir = temp_data_dir
    df = pd.DataFrame([
        ["2026-01-01 10:00:00", "ORD1", "ORD1_1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
        ["2026-01-01 10:00:00", "ORD1", "ORD1_1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
        ["2026-01-01 10:05:00", "ORD2", "ORD2_1", "Casque Gamer", "Informatique", 1, 55, 55, "Lyon", "CUST2"],
    ], columns=["Date", "IDCommande", "IDLigne", "Produit", "Categorie", "Quantite", "Prix", "Montant", "Region", "Client"])
    df.to_csv(pipeline.SOURCE_FILE, index=False)

    ti = FakeTaskInstance()
    branch = pipeline.data_quality_check(ti=ti)
    assert branch == "quality_status_partial"
    assert ti.xcom_pull(key="nb_lignes_rejetees") == 1
    assert ti.xcom_pull(key="nb_lignes_valides") == 1


def test_data_quality_all_invalid_returns_failed(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 0, 1200, -1200, "Paris", "CUST1"],
    ])
    ti = FakeTaskInstance()
    branch = pipeline.data_quality_check(ti=ti)
    assert branch == "quality_status_failed"

# Tests : calcul des KPI

def test_compute_kpis(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
        ["2026-01-05 10:05:00", "ORD2", "Souris Logitech", "Informatique", 2, 25, 50, "Lyon", "CUST2"],
        ["2026-02-01 10:05:00", "ORD3", "Casque Gamer", "Informatique", 1, 55, 55, "Paris", "CUST1"],
    ])
    ti = FakeTaskInstance()
    pipeline.data_quality_check(ti=ti)
    pipeline.load_data(ti=ti)
    pipeline.compute_kpis(ti=ti)

    global_metrics = ti.xcom_pull(key="global_metrics")
    assert global_metrics["nb_commandes"] == 3
    assert global_metrics["nb_clients"] == 2
    assert global_metrics["chiffre_affaires"] == 1305.0
    assert round(global_metrics["panier_moyen"], 2) == round(1305 / 3, 2)

    region_metrics = ti.xcom_pull(key="region_metrics")
    assert any(r["region"] == "Paris" for r in region_metrics)


# Tests : analyse par categorie (tache dynamique)

def test_analyse_categorie_returns_expected_metrics(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
        ["2026-01-01 10:05:00", "ORD2", "Routeur WiFi", "Reseau", 1, 65, 65, "Lyon", "CUST2"],
    ])
    ti = FakeTaskInstance()
    pipeline.data_quality_check(ti=ti)
    pipeline.load_data(ti=ti)

    resultat = pipeline.analyse_categorie("Informatique", ti=ti)
    assert resultat["categorie"] == "Informatique"
    assert resultat["chiffre_affaires"] == 1200.0
    assert resultat["nb_commandes"] == 1


def test_analyse_categorie_skips_when_empty(temp_data_dir):
    from airflow.exceptions import AirflowSkipException
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
    ])
    ti = FakeTaskInstance()
    pipeline.data_quality_check(ti=ti)
    pipeline.load_data(ti=ti)

    with pytest.raises(AirflowSkipException):
        pipeline.analyse_categorie("Logiciels", ti=ti)



# Tests : generation du rapport final

def test_generate_final_report_status_success(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
    ])
    ti = FakeTaskInstance()
    pipeline.data_quality_check(ti=ti)
    pipeline.load_data(ti=ti)
    pipeline.compute_kpis(ti=ti)
    ti.xcom_push(key="category_metrics", value=[])

    rapport = pipeline.generate_final_report(ti=ti, ds="2026-06-18")
    assert rapport["status"] == "success"
    assert rapport["quality"]["invalid_rows"] == 0
    assert os.path.isfile(os.path.join(tmp_dir, "rapport_2026-06-18.json"))


def test_generate_final_report_status_partial(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", 1, 1200, 1200, "Paris", "CUST1"],
        ["2026-01-01 10:05:00", "ORD2", "Souris Logitech", "Informatique", -1, 25, -25, "Lyon", "CUST2"],
    ])
    ti = FakeTaskInstance()
    pipeline.data_quality_check(ti=ti)
    pipeline.load_data(ti=ti)
    pipeline.compute_kpis(ti=ti)
    ti.xcom_push(key="category_metrics", value=[])

    rapport = pipeline.generate_final_report(ti=ti, ds="2026-06-18")
    assert rapport["status"] == "partial"
    assert rapport["quality"]["invalid_rows"] == 1


def test_generate_final_report_status_failed_when_no_valid_rows(temp_data_dir):
    pipeline, tmp_dir = temp_data_dir
    make_sample_csv(pipeline.SOURCE_FILE, [
        ["2026-01-01 10:00:00", "ORD1", "Laptop Dell XPS", "Informatique", -1, 1200, -1200, "Paris", "CUST1"],
    ])
    ti = FakeTaskInstance()
    pipeline.data_quality_check(ti=ti)
    ti.xcom_push(key="global_metrics", value={})
    ti.xcom_push(key="category_metrics", value=[])

    rapport = pipeline.generate_final_report(ti=ti, ds="2026-06-18")
    assert rapport["status"] == "failed"
