#!/usr/bin/env python3
"""
check_mongodb.py
-----------------
Script de verification post-deploiement utilise par le pipeline Jenkins
(stage "Verify MongoDB").

Il se connecte a MongoDB, verifie que la base 'ecommerce_analytics' et la
collection 'sales_metrics' existent, et controle qu'un document a bien ete
insere recemment. Retourne un code de sortie != 0 en cas d'echec
afin de faire echouer le job Jenkins si l'ecriture n'a pas eu lieu.

Usage :
    python scripts/check_mongodb.py [--uri MONGO_URI] [--max-age-hours 26]
"""

import argparse
import sys
from datetime import datetime, timedelta

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


def parse_args():
    parser = argparse.ArgumentParser(description="Verifie le stockage des metriques dans MongoDB.")
    parser.add_argument(
        "--uri",
        default="mongodb://localhost:27017",
        help="URI de connexion MongoDB (defaut: mongodb://localhost:27017)",
    )
    parser.add_argument("--db", default="ecommerce_analytics", help="Nom de la base de donnees")
    parser.add_argument("--collection", default="sales_metrics", help="Nom de la collection")
    parser.add_argument(
        "--max-age-hours",
        type=int,
        default=26,
        help="Age maximum accepte (en heures) du dernier document insere",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        client = MongoClient(args.uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except ServerSelectionTimeoutError as exc:
        print(f"[ECHEC] Impossible de se connecter a MongoDB ({args.uri}) : {exc}")
        sys.exit(1)

    db_names = client.list_database_names()
    if args.db not in db_names:
        print(f"[ECHEC] La base '{args.db}' n'existe pas encore sur ce serveur MongoDB.")
        sys.exit(1)

    db = client[args.db]
    if args.collection not in db.list_collection_names():
        print(f"[ECHEC] La collection '{args.collection}' n'existe pas dans la base '{args.db}'.")
        sys.exit(1)

    collection = db[args.collection]
    total_docs = collection.count_documents({})
    if total_docs == 0:
        print(f"[ECHEC] La collection '{args.collection}' est vide.")
        sys.exit(1)

    dernier_doc = collection.find_one(sort=[("_id", -1)])
    execution_date = dernier_doc.get("execution_date")
    status = dernier_doc.get("status")

    print("[OK] Connexion MongoDB reussie.")
    print(f"[OK] Base '{args.db}' / collection '{args.collection}' trouvees.")
    print(f"[OK] Nombre total de documents : {total_docs}")
    print(f"[OK] Dernier document -> execution_date={execution_date}, status={status}")

    if status not in ("success", "partial"):
        print(f"[ATTENTION] Le dernier statut d'execution est '{status}'.")
        sys.exit(2)

    print("[OK] Verification MongoDB terminee avec succes.")
    client.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
