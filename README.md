# 🚖 PFE-M Big Data 2026 — Sujet 3 : Le Gardien de la Donnée

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-4.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Google Colab](https://img.shields.io/badge/Google_Colab-F9AB00?style=for-the-badge&logo=googlecolab&logoColor=white)
![Status](https://img.shields.io/badge/Status-Completed-2ea44f?style=for-the-badge)

**Data Quality Framework Distribué sur le Dataset NYC Yellow Taxi**  
* Master Big Data | 2026*

</div>

---

## 📋 Table des matières

- [À propos du projet](#-à-propos-du-projet)
- [Contexte : La SMELEC](#-contexte--la-smelec)
- [Dataset](#-dataset)
- [Architecture du pipeline](#-architecture-du-pipeline)
- [Structure du projet](#-structure-du-projet)
- [Installation & Utilisation](#-installation--utilisation)
- [Résultats obtenus](#-résultats-obtenus)
- [Analyse de performance](#-analyse-de-performance)
- [Technologies utilisées](#-technologies-utilisées)

---

## 🎯 À propos du projet

Ce projet implémente un **moteur de validation de données distribué** (Data Quality Framework) capable de traiter des volumes massifs de l'ordre du téraoctet. Il démontre que la qualité de la donnée est le premier verrou de toute prise de décision fiable — illustré par le principe fondamental :

> *"Garbage In, Garbage Out"*

Le pipeline détecte et élimine automatiquement les valeurs aberrantes, incohérences logiques et données manquantes sur **3 066 766 enregistrements** de trajets NYC Taxi.

---

## 🇲🇷 Contexte : La SMELEC

Ce projet simule un **filtre anti-erreurs de facturation** pour la Société Mauritanienne d'Electricité (SMELEC).

| Problème réel | Solution apportée |
|---|---|
| Compteur affichant 5 000 kWh/jour suite à bug | Détection par Z-Score → blocage automatique |
| Facturation erronée → réclamations citoyennes | Pipeline de validation avant émission facture |
| Capteur défaillant non identifié | Analyse biais par fournisseur (VendorID) |
| Statistiques nationales faussées | Nettoyage garantissant la véracité des données |

---

## 📦 Dataset

| Propriété | Valeur |
|---|---|
| **Source** | [NYC TLC Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| **Période** | Janvier 2023 |
| **Format** | Parquet |
| **Lignes** | 3 066 766 |
| **Colonnes** | 19 + 2 calculées |
| **Colonnes clés** | `fare_amount`, `trip_distance`, `passenger_count`, `tip_amount`, `speed_kmh` |

---

## 🏗️ Architecture du pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                    NYC YELLOW TAXI DATASET                   │
│                       3 066 766 lignes                       │
└──────────────────────────┬──────────────────────────────────┘
                           │
                    ┌──────▼──────┐
                    │  CELLULE 2  │
                    │  Chargement │  spark.read.parquet()
                    │  HDFS/Drive │  + colonnes calculées
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  CELLULE 3  │
                    │  Profilage  │  df.summary() — 1 seule passe
                    │  Statistiq. │  mean, stddev, min, max, Q1/Q3
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  CELLULE 4  │
                    │   Z-Score   │  Z = (x − μ) / σ
                    │  Détection  │  Seuil : |Z| > 3
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  CELLULE 5  │
                    │  Nettoyage  │  5 règles logiques + dropna
                    │  Dirty Data │  Lazy evaluation Spark
                    └──────┬──────┘
                           │
             ┌─────────────┴─────────────┐
             │                           │
      ┌──────▼──────┐             ┌──────▼──────┐
      │  CELLULE 6  │             │  CELLULE 7  │
      │ Comparaison │             │ Taux Rejet  │
      │ Avant/Après │             │ Biais Capt. │
      └──────┬──────┘             └──────┬──────┘
             │                           │
             └─────────────┬─────────────┘
                           │
                    ┌──────▼──────┐
                    │  CELLULE 8  │
                    │Performance  │  CPU / RAM / IO / Benchmark
                    │  CPU/RAM    │  df.explain()
                    └─────────────┘
                           │
                    ┌──────▼──────┐
                    │   OUTPUT    │
                    │ 2 847 679   │  lignes validées (-7.14%)
                    │   lignes    │  prêtes pour analyse
                    └─────────────┘
```

---

## 📁 Structure du projet

```
PFE-M-BigData2026-Sujet3/
│
├── 📓 PFE_Sujet3_DataCleaning.ipynb    # Notebook principal Google Colab
│
├── 📄 README.md                         # Ce fichier
│
├── 📊 Rapport_PFE_Sujet3_DataCleaning.docx  # Rapport complet (20 pages)
│
└── 📸 screenshots/                      # Captures Spark UI
    ├── cellule3_profiling.png
    ├── cellule4_zscore.png
    ├── cellule6_comparison.png
    └── cellule8_performance.png
```

---

## 🚀 Installation & Utilisation

### Option 1 — Google Colab (recommandé)

1. Cloner le dépôt :
```bash
git clone https://github.com/votre-username/PFE-M-BigData2026-Sujet3.git
```

2. Ouvrir Google Colab : [colab.research.google.com](https://colab.research.google.com)

3. **Fichier → Importer le notebook** → sélectionner `PFE_Sujet3_DataCleaning.ipynb`

4. Exécuter toutes les cellules : `Ctrl + F9`

> ⚠️ **Important** : La cellule 1 doit être réexécutée à chaque reconnexion Colab.

### Option 2 — Environnement local

```bash
# Prérequis : Java 11 + Python 3.10+
pip install pyspark==4.0.0

# Lancer Jupyter
jupyter notebook PFE_Sujet3_DataCleaning.ipynb
```

### Sauvegarder le dataset sur Google Drive (évite le re-téléchargement)

```python
from google.colab import drive
drive.mount('/content/drive')

# Le dataset sera sauvegardé une seule fois
drive_path = "/content/drive/MyDrive/nyc_taxi_2023_01.parquet"
```

---

## 📊 Résultats obtenus

### Statistiques du profilage

| Colonne | Moyenne | Écart-type | Min | Max |
|---|---|---|---|---|
| `fare_amount` | $18.37 | $17.81 | -$900.0 | $1 160.1 |
| `trip_distance` | 3.85 mi | 249.58 | 0.0 | 258 928 mi |
| `passenger_count` | 1.36 | 0.90 | 0 | 9 |
| `speed_kmh` | 25.33 | 2 895.03 | -15.95 | 4 167 034 |

### Détection des outliers (Z-Score, seuil |Z| > 3)

| Colonne | Outliers | Pourcentage |
|---|---|---|
| `fare_amount` | 35 090 | 1.14% |
| `trip_distance` | 67 | 0.00% |
| **Total** | **35 154** | **1.15%** |

### Résultats du nettoyage

| Indicateur | Valeur |
|---|---|
| Lignes initiales | 3 066 766 |
| Lignes après nettoyage | 2 847 679 |
| Lignes rejetées | 219 087 |
| **Taux de rejet** | **7.14%** |
| Temps d'exécution | 12.65 secondes |

### Impact statistique (Avant → Après nettoyage)

| Indicateur | AVANT | APRÈS | Δ Erreur |
|---|---|---|---|
| Moyenne `fare_amount` | $18.37 | $17.80 | **3.08%** |
| Variance `fare_amount` | $317.12 | $219.55 | **30.77%** |

### Biais capteur détecté

| VendorID | Outliers | Part |
|---|---|---|
| Vendor 1 (Creative Mobile) | 6 268 | 17.8% |
| **Vendor 2 (VeriFone)** | **28 886** | **82.2%** |

> 💡 **Conclusion** : Le Vendor 2 génère 82.2% des erreurs — biais de capteur systématique identifié.

---

## ⚙️ Analyse de performance

| Ressource | Valeur | Commentaire |
|---|---|---|
| RAM totale | 12.7 Go | Cluster Google Colab |
| RAM utilisée | 3.9 Go | 32% de la RAM totale |
| RAM processus Spark | 113 Mo | Empreinte légère du driver |
| Cœurs CPU | 2 | Architecture dual-core |
| Utilisation CPU | 54.8% | Pipeline bien parallélisé |
| Temps profilage | ~45 s | O(n) — une seule passe |
| Temps nettoyage | 12.65 s | O(n) — lazy evaluation |
| Temps Z-Score | ~8 s | O(n) — agrégation distribuée |

---

## 🛠️ Technologies utilisées

| Technologie | Version | Usage |
|---|---|---|
| Python | 3.10+ | Langage principal |
| Apache Spark | 4.0 | Moteur de traitement distribué |
| PySpark | 4.0 | Interface Python de Spark |
| Java (OpenJDK) | 11 | Runtime requis par Spark |
| Google Colab | — | Environnement d'exécution cloud |
| Parquet | — | Format de stockage colonnaire |

---

## 📐 Règles de nettoyage appliquées

```python
# Règle 1 : Tarif strictement positif
df_clean = df_clean.filter(col("fare_amount") > 0)

# Règle 2 : Distance strictement positive
df_clean = df_clean.filter(col("trip_distance") > 0)

# Règle 3 : Vitesse physiquement possible (< 200 km/h)
df_clean = df_clean.filter((col("speed_kmh") < 200) & (col("speed_kmh") > 0))

# Règle 4 : Durée de trajet entre 1 minute et 6 heures
df_clean = df_clean.filter(
    (col("trip_duration_h") >= (1/60)) & (col("trip_duration_h") <= 6)
)

# Règle 5 : Nombre de passagers entre 1 et 6
df_clean = df_clean.filter(col("passenger_count").between(1, 6))
```

---

## 📚 Références

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- Cahier des charges : *PFE-M 2026 Big Data — LIU Master*

---

<div align="center">

**LIU — Master Big Data 2026**  
Sujet 3 : Le Gardien de la Donnée

</div>
