# Cartographie militants



Ce script prend en entrée un fichier CSV des listes de militants, et le convertit en fichier GeoJson à utiliser directement sur OpenStreetMap, Google Earth, etc.



## Prérequis



* Installation de Python3
* Installation des librairies requests, tqdm et geopy

```bash
pip install requests tqdm geopy
```

## Instructions

Lancer la commande suivante

```bash
python csv\_to\_umap\_geojson\_militants.py --input "ListeMilitants.csv"
```

## Arguments

| Argument  | Raccourci | Requis | Description | Valeur par défaut |
| --- | --- | --- | --- | --- |
| --input | -i | ✅ | CSV d'entrée | Aucune |

* Dans les dossiers horodatés:



| Fichier  | Contenu |
| --- | --- |
| output\_umap.geojson | Points géocodés |
| output\_not\_geocoded.geojson | Adresses introuvables |
| output\_incomplete.geojson | Adresses incomplètes |
| output\_duplicates.geojson | Doublons |
| geocache\_added.json | Nouvelles entrées ajoutées au cache global lors de CE RUN |
| geocache\_new\_added.json | Nouvelles entrées ajoutées au cache cumulatif lors de CE RUN |
| quality\_report.csv | Statistiques globales |
| problematic\_rows.csv | Listing de toutes les lignes problématiques |