# Pipeline E-commerce — Industrialisation avec Git, Jenkins, Airflow et MongoDB

Projet de Data Engineering — Master Informatique
Industrialisation d'un pipeline d'analyse des ventes pour une entreprise
e-commerce spécialisée dans la vente de produits informatiques.

## 1. Contexte

L'entreprise traitait ses données de vente manuellement à partir de fichiers
CSV exportés quotidiennement. Ce projet met en place une plateforme
automatisée : collecte → contrôle qualité → calcul d'indicateurs → stockage
→ reporting, orchestrée par Apache Airflow, déployée via Jenkins et
conteneurisée avec Docker.

## 2. Architecture

```
Développeur → Git → Jenkins → Tests / Validation DAG / Déploiement / Trigger
                                        ↓
                                 Apache Airflow
                                        ↓
                     Extraction → Transformation → Calcul KPI
                                        ↓
                                    MongoDB
```

## 3. Structure du dépôt

```
airflow-ecommerce-project/
├── dags/
│   └── ecommerce_sales_pipeline.py   # DAG principal (21 tâches)
├── tests/
│   └── test_pipeline.py              # 17 tests unitaires (pytest)
├── data/
│   └── dataset.csv                   # Jeu de données (voir section 4)
├── scripts/
│   └── check_mongodb.py              # Script de vérification MongoDB (Jenkins)
├── docker/
│   └── Dockerfile.airflow            # Image Airflow personnalisée
├── Jenkinsfile                       # Pipeline CI/CD (7 stages)
├── requirements.txt
├── docker-compose.yml                # Jenkins + Airflow + PostgreSQL + MongoDB
└── README.md
```

## 4. Jeu de données

Le projet utilise le **dataset réel Olist Brazilian E-Commerce**
(https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), fourni par
l'utilisateur (9 fichiers CSV : commandes, articles, produits, clients,
paiements, avis, vendeurs, géolocalisation, traduction des catégories).

`data/dataset.csv` est le résultat d'un **script de mapping** qui reconstitue
le schéma cible attendu à partir de plusieurs tables sources Olist :

| Champ cible | Origine Olist |
|---|---|
| IDLigne | généré : `order_id` + rang de la ligne dans la commande (clé unique de ligne) |
| Date | `orders.order_purchase_timestamp` |
| IDCommande | `orders.order_id` |
| Produit | `order_items.product_id` (raccourci en SKU-xxxxxxxxxx) |
| Categorie | `products.product_category_name`, traduit en anglais via `product_category_name_translation.csv` |
| Quantité | nombre de lignes `order_items` pour le couple (commande, produit) |
| Prix / Montant | `order_items.price` (prix unitaire / montant agrégé) |
| Région | `customers.customer_state`, converti en nom d'état complet |
| Client | `customers.customer_unique_id` |

**Périmètre retenu :** seules les commandes au statut `delivered` (ventes
effectivement livrées) et les catégories liées à l'informatique / l'électronique
ont été conservées, afin de rester cohérent avec le contexte métier de
l'entreprise cible (vente de produits informatiques) :
`Computers`, `Computers Accessories`, `Electronics`, `Consoles Games`,
`Audio`, `Fixed Telephony`, `Tablets Printing Image`, `Telephony`.
Cela représente **15 286 lignes de vente réelles**, 14 958 commandes et
14 742 clients distincts, sur la période octobre 2016 – août 2018.

**Point d'attention traité (donnée réelle Olist) :** une commande peut
légitimement contenir plusieurs articles différents (même `IDCommande` répété
sur plusieurs lignes). Le contrôle qualité du DAG utilise donc une clé
d'unicité dédiée (`IDLigne`) plutôt que `IDCommande` seul pour détecter les
véritables doublons, avec repli automatique sur `IDCommande` si la colonne
`IDLigne` est absente (voir `dags/ecommerce_sales_pipeline.py::_line_key`).

## 5. Le DAG Airflow (`ecommerce_sales_pipeline`)

Le DAG s'exécute quotidiennement (`@daily`) et enchaîne :

1. **wait_for_file** — `FileSensor` : attend la présence du CSV.
2. **verify_file_exists** — vérifie l'existence physique du fichier.
3. **verify_file_not_empty** — vérifie que le fichier contient des lignes.
4. **data_quality_check** — `BranchPythonOperator` : applique les règles de
   gestion métier (montant négatif, quantité invalide, ID dupliqué), isole
   les lignes rejetées dans `errors.csv`, et route vers `success`,
   `partial` ou `failed` selon le résultat.
5. **join_after_quality** — point de jonction (`TriggerRule.ONE_SUCCESS`)
   qui poursuit le pipeline si le statut est `success` ou `partial`.
6. **load_data** — charge uniquement les lignes valides.
7. **compute_kpis** — calcule les indicateurs globaux et les transmet par
   **XComs** (commandes, clients, CA, panier moyen, top produits, CA par
   région, évolution mensuelle).
8. **analyse_categorie_\*** — **tâches générées dynamiquement**, une par
   catégorie de produit (8 tâches), chacune calculant son propre
   sous-ensemble d'indicateurs.
9. **aggregate_category_metrics** — agrège les résultats des tâches
   dynamiques (`TriggerRule.ALL_DONE`, tolère les catégories vides/skip).
10. **generate_final_report** — génère un rapport JSON final
    (`TriggerRule.ALL_DONE` : s'exécute même si le contrôle qualité a
    échoué, afin d'historiser chaque exécution avec son statut).
11. **store_metrics_mongodb** — insère le document de métriques dans
    MongoDB (`ecommerce_analytics.sales_metrics`).

### Gestion des erreurs et Trigger Rules

- `quality_status_failed` utilise `ALL_DONE` pour se déclencher même en cas
  d'échec du contrôle qualité (afin d'alimenter le rapport final).
- `join_after_quality` utilise `ONE_SUCCESS` pour ne poursuivre le pipeline
  que si au moins une branche (`success` ou `partial`) a été retenue.
- `aggregate_category_metrics` et `generate_final_report` utilisent
  `ALL_DONE` afin qu'un statut `partial` ou `failed` en amont n'empêche pas
  la génération et l'historisation du rapport.
- Si le fichier est vide ou totalement invalide, le DAG s'arrête proprement
  au niveau du contrôle qualité (branche `failed`) sans planter, et le
  statut `failed` est tracé dans MongoDB.

## 6. Indicateurs métier calculés

- Nombre total de commandes et de clients
- Chiffre d'affaires total et panier moyen
- Top 10 des produits les plus vendus
- Chiffre d'affaires par catégorie (8 catégories réelles : Computers Accessories, Telephony, Electronics, Consoles Games, Audio, Fixed Telephony, Computers, Tablets Printing Image) et par région (27 états brésiliens)
- Évolution des ventes par mois
- Nombre de lignes valides / rejetées

## 7. Stockage MongoDB

Base : `ecommerce_analytics` — Collection : `sales_metrics`.
Un document est inséré à chaque exécution du DAG, avec le statut
(`success`, `partial`, `failed`), les métriques globales, le top produits,
les métriques par région/catégorie, et les statistiques de qualité —
conforme au schéma défini dans l'énoncé du projet.

## 8. Pipeline Jenkins (CI/CD)

Le `Jenkinsfile` définit 7 stages :

1. **Checkout** — récupération du code depuis Git
2. **Install dependencies** — `pip install -r requirements.txt` dans un venv
3. **Run tests** — `pytest tests/ -v` (17 tests unitaires)
4. **Validate DAG** — `python -m py_compile dags/*.py`
5. **Deploy DAG** — copie du DAG vers le dossier `dags/` d'Airflow
6. **Trigger DAG** — `airflow dags trigger ecommerce_sales_pipeline`
7. **Verify MongoDB** — `scripts/check_mongodb.py` contrôle qu'un document
   récent avec un statut valide a bien été inséré

## 9. Conteneurisation (Docker)

`docker-compose.yml` orchestre :

- **postgres** — base de métadonnées Airflow
- **airflow-init / airflow-webserver / airflow-scheduler** — image
  personnalisée (`docker/Dockerfile.airflow`) basée sur `apache/airflow`
  avec les dépendances du projet
- **mongodb** — stockage des indicateurs
- **jenkins** — orchestration CI/CD

### Démarrage local

```bash
docker compose up -d --build
# Airflow UI  : http://localhost:8080  (admin / admin)
# Jenkins UI  : http://localhost:8081
# MongoDB     : localhost:27017
```

## 10. Exécution des tests en local

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

17 tests unitaires couvrent : détection de fichier manquant/vide, règles de
gestion métier (montant négatif, quantité invalide, doublons de ligne via
IDLigne, gestion correcte des commandes multi-articles), calcul des KPI,
tâches dynamiques par catégorie, et génération du rapport final
(statuts `success` / `partial` / `failed`).

## 11. Gestion Git

Le dépôt suit un workflow à deux branches :

- `main` — branche stable
- `dev` — branche de développement

Les fonctionnalités sont développées sur `dev` puis fusionnées vers `main`
après validation (voir historique des commits : `git log --oneline --graph --all`).

## 12. Difficultés rencontrées

- Reconstitution du schéma cible à partir de plusieurs tables Olist normalisées (commandes, articles, produits, clients, traduction des catégories) : nécessité d'un script de jointure/agrégation dédié plutôt qu'un simple renommage de colonnes.
- **Commandes multi-articles** : contrairement à l'hypothèse initiale d'un identifiant de commande unique par ligne, les données réelles Olist montrent qu'une même commande (`IDCommande`) peut légitimement comporter plusieurs lignes de produits différents. La règle de détection des doublons a dû être adaptée pour s'appuyer sur une clé de ligne dédiée (`IDLigne`) plutôt que sur l'identifiant de commande seul, afin de ne pas rejeter à tort des ventes valides.
- Compatibilité des versions (`werkzeug`/`flask`/`sqlalchemy`) lors de l'installation locale d'Airflow pour les tests CI.
- Choix du périmètre de catégories : le dataset Olist couvre des dizaines de catégories généralistes (mode, meubles, beauté...) ; un filtrage sur les catégories informatique/électronique a été nécessaire pour rester cohérent avec le contexte métier de l'entreprise cible.

## 13. Auteurs

Groupe Data Engineering — Master Informatique — Juin 2026
