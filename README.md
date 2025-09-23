# kafka-ipssi

Repo d’exercices Kafka/Spark (une branche = un exercice).

## Prérequis
- Docker + Docker Compose
- Python 3.10+ (idéalement 3.11)
- Git

## Installation rapide
```bash
python -m venv .venv && . .venv/Scripts/activate  # Windows PowerShell
pip install -r requirements.txt
docker compose up -d

## HDFS (exercice 7)

Le `docker-compose.yml` fournit désormais un NameNode et un DataNode Hadoop.
Pour lancer Kafka + HDFS en local :

```bash
docker compose up -d kafka hdfs-namenode hdfs-datanode
```

L’interface WebHDFS/Namenode est exposée sur http://localhost:9870 et le RPC
sur le port `9000`. Lors du premier lancement, vous pouvez préparer la racine
de stockage attendue par l’exercice :

```bash
docker exec -it hdfs-namenode hdfs dfs -mkdir -p /hdfs-data
```

Le consommateur `consumer_hdfs.py` se connecte ensuite via WebHDFS :

```bash
python consumer_hdfs.py \
  --hdfs-url http://localhost:9870 \
  --hdfs-base-path /hdfs-data
```

Sans option `--hdfs-url`, le script retombe sur un stockage local dans le
dossier `--base-dir` (par défaut `/hdfs-data`).
