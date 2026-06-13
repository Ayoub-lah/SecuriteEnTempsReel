# 🔐 Détection d'Anomalies de Sécurité en Temps Réel

> Pipeline Big Data complet de détection d'anomalies sur flux de sécurité en temps réel  
> **Architecture Kappa** — Kafka + Spark Structured Streaming + PostgreSQL + Streamlit

---

## 📋 Table des matières

- [Contexte et objectifs](#-contexte-et-objectifs)
- [Architecture](#-architecture)
- [Stack technique](#-stack-technique)
- [Structure du projet](#-structure-du-projet)
- [Dataset](#-dataset-nsl-kdd)
- [Installation et lancement](#-installation-et-lancement)
- [Résultats obtenus](#-résultats-obtenus)
- [Auteurs](#-auteurs)

---

## 🎯 Contexte et objectifs

Les systèmes d'information de sécurité (pare-feu, IDS/IPS, serveurs d'authentification) produisent en continu d'énormes volumes de journaux. La détection d'une attaque ou d'un comportement anormal n'a de valeur que si elle est **quasi immédiate**.

### Objectif
Concevoir et réaliser un pipeline Big Data complet qui :
- **Collecte** des événements de sécurité en temps réel
- **Ingère** les données via Apache Kafka
- **Traite** le flux en temps réel avec Spark Structured Streaming
- **Détecte** les comportements anormaux automatiquement
- **Affiche** les alertes sur un tableau de bord en temps réel

---

## 🏗️ Architecture

Le projet utilise une **Architecture Kappa** — contrairement à Lambda qui maintient deux couches séparées (Batch et Speed), Kappa fusionne les deux et fait transiter toutes les données par un flux unique.

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   NSL-KDD   │    │ Producteur  │    │    Kafka    │    │    Spark    │    │ PostgreSQL  │
│  (Dataset)  │───▶│   Python   │───▶│  (Broker)  │───▶│ Streaming  │───▶│  + Parquet  │
│ 125,973 lg  │    │  50ms/ligne │    │  security- │    │  10s batch  │    │  Stockage   │
└─────────────┘    └─────────────┘    │   events   │    └─────────────┘    └──────┬──────┘
                                      └─────────────┘                             │
                                                                                   ▼
                                                                          ┌─────────────────┐
                                                                          │    Dashboard    │
                                                                          │   Streamlit     │
                                                                          │  localhost:8501 │
                                                                          └─────────────────┘
```

### Les 5 étapes du pipeline

| Étape | Composant | Rôle |
|-------|-----------|------|
| 1️⃣ Source | NSL-KDD Dataset | 125,973 connexions réseau réelles |
| 2️⃣ Ingestion | Producteur Python | Lit et envoie les données dans Kafka |
| 3️⃣ Messagerie | Apache Kafka | Reçoit, stocke et distribue les événements |
| 4️⃣ Traitement | Apache Spark | Analyse et détecte les anomalies en temps réel |
| 5️⃣ Stockage | PostgreSQL + Parquet | Stockage temps réel + historique brut |
| 6️⃣ Visualisation | Streamlit | Tableau de bord rafraîchi toutes les 5 secondes |

---

## 🛠️ Stack technique

| Composant | Version | Rôle |
|-----------|---------|------|
| 🐳 Docker Compose | Latest | Orchestration de tous les services |
| 📨 Apache Kafka | 3.7.0 | Broker de messages (ingestion) |
| ⚡ Apache Spark | 3.5.1 | Traitement temps réel (Structured Streaming) |
| 🐍 Python | 3.11 | Producteur de données + scripts |
| 🗄️ PostgreSQL | 16 | Stockage des vues temps réel |
| 📁 Parquet | - | Stockage historique brut compressé |
| 📊 Streamlit | Latest | Tableau de bord temps réel |

> ✅ **100% Open-Source et Gratuit** — Aucun composant payant, aucune offre cloud

---

## 📁 Structure du projet

```
projet-bigdata/
│
├── 📄 docker-compose.yml          # Orchestration des services
│
├── 📂 producer/
│   ├── producer.py                # Producteur Python → Kafka
│   └── requirements.txt           # Dépendances Python
│
├── 📂 spark/
│   ├── spark_processor.py         # Traitement Spark Structured Streaming
│   ├── lire_parquet.py            # Script de lecture des fichiers Parquet
│   └── historique/                # Fichiers Parquet (historique brut)
│
├── 📂 dashboard/
│   └── app.py                     # Tableau de bord Streamlit
│
├── 📂 postgres-init/
│   └── init.sql                   # Initialisation de la base de données
│
└── 📂 data/
    └── KDDTrain+.txt              # Dataset NSL-KDD (à télécharger)
```

---

## 📊 Dataset NSL-KDD

Le dataset **NSL-KDD** est un dataset public de cybersécurité contenant des connexions réseau réelles, avec des connexions normales et des attaques.

### Téléchargement
```bash
cd data/
wget https://raw.githubusercontent.com/defcom17/NSL_KDD/master/KDDTrain+.txt
```

### Types d'attaques détectées

| Attaque | Description | Type |
|---------|-------------|------|
| `neptune` | Déni de service (SYN flood) | DoS |
| `satan` | Scan de vulnérabilités | Probe |
| `ipsweep` | Scan d'adresses IP | Probe |
| `portsweep` | Scan de ports | Probe |
| `smurf` | Inondation réseau | DoS |
| `nmap` | Scan réseau | Probe |
| `back` | Attaque Apache | DoS |
| `teardrop` | Fragmentation réseau | DoS |
| `warezclient` | Téléchargement illégal | R2L |

---

## 🚀 Installation et lancement

### Prérequis
- Docker et Docker Compose installés
- 4 Go de RAM minimum
- Connexion internet (première fois uniquement)

---

### Étape 1 — Cloner le projet
```bash
git clone https://github.com/Ayoub-lah/SecuriteEnTempsReel.git
cd SecuriteEnTempsReel
```

### Étape 2 — Télécharger le dataset
```bash
cd data/
wget https://raw.githubusercontent.com/defcom17/NSL_KDD/master/KDDTrain+.txt
cd ..
```

### Étape 3 — Lancer les services Docker
```bash
docker compose up -d
```

Vérifier que tout tourne :
```bash
docker compose ps
```

Résultat attendu :
```
NAME        STATUS
kafka       Up
postgres    Up
spark       Up
dashboard   Up
```

### Étape 4 — Lancer le Producteur Python
```bash
docker run -it --rm \
  --network projet-bigdata_default \
  -v $(pwd)/producer:/app \
  -v $(pwd)/data:/data \
  -w /app \
  python:3.11-slim \
  bash -c "pip install kafka-python -q && python producer.py"
```

### Étape 5 — Lancer Spark Structured Streaming
```bash
docker run -it --rm \
  --network projet-bigdata_default \
  -v $(pwd)/spark:/opt/spark-apps \
  -v $(pwd)/data:/data \
  --user root \
  apache/spark:3.5.1 \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
  /opt/spark-apps/spark_processor.py
```

### Étape 6 — Ouvrir le Dashboard
```
http://localhost:8501
```

### Arrêter le projet
```bash
docker compose down
```

---

## 📈 Résultats obtenus

### Métriques du pipeline

| Métrique | Valeur |
|----------|--------|
| 📦 Total événements analysés | 26,735 |
| 🚨 Total attaques détectées | 12,476 |
| ⚠️ Taux d'attaques | 46.7% |
| ⚡ Latence de détection | < 15 secondes |
| 💾 Fichiers Parquet générés | 68+ batches |

### Distribution des attaques

```
neptune        ████████████████████  3,867
normal         ████████████████      6,276
satan          ████                    310
ipsweep        ███                     300
portsweep      ███                     272
smurf          ██                      255
nmap           ██                      140
teardrop       █                        90
back           █                        86
warezclient    █                        73
autres         █                        66
```

### Stockage double

| Type | Technologie | Utilisation |
|------|-------------|-------------|
| Temps réel | PostgreSQL | Dashboard et alertes |
| Historique | Parquet (Snappy) | Rejeu et analyse |

---

## 👥 Auteurs

| Rôle | Étudiant | Responsabilité |
|------|----------|----------------|
| 🔧 Ingestion | Étudiant 1 | Kafka + Producteur Python |
| ⚡ Traitement | Étudiant 2 | Spark Structured Streaming |
| 📊 Visualisation | Étudiant 3 | Dashboard + PostgreSQL |

---

## 📚 Références

- [Documentation Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Documentation Apache Kafka](https://kafka.apache.org/documentation/)
- [Dataset NSL-KDD](https://www.unb.ca/cic/datasets/nsl.html)

---

## 📄 Licence

Projet académique — Master Sécurité IT & BigData  
Module : Architecture et Technologies BigData  
Département Génie Informatique

---

> 🔐 *"La sécurité n'est pas un produit, c'est un processus."* — Bruce Schneier
