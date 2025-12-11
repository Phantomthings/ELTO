# ELTO Dashboard Export

Version exportable du dashboard ELTO pour intégration dans une application Laravel PHP.

## Fonctionnalités incluses

- **Filtres dynamiques** : Sites, Dates, Types d'erreur, Moments d'erreur
- **Générale** : Vue d'ensemble des charges avec taux de réussite par site
- **Détails Site** : Analyse détaillée par site et PDC
- **Analyse Erreur** : Top 3 erreurs EVI/Downstream avec détail par site

## Structure

```
EXPORT/
├── main.py                 # Point d'entrée FastAPI (sans auth)
├── db.py                   # Connexion MySQL
├── requirements.txt        # Dépendances Python
├── routers/
│   ├── filters.py          # API filtres dynamiques
│   └── sessions.py         # API général/détails/analyse
├── templates/
│   ├── index.html          # Page principale (HTML pur)
│   └── partials/
│       ├── sessions_general.html
│       ├── sessions_site_details.html
│       └── error_analysis.html
├── static/
│   ├── css/
│   │   └── style.css       # Styles CSS
│   └── js/
│       ├── app.js          # Application principale
│       ├── filters.js      # Gestion des filtres
│       ├── charts.js       # Graphiques SVG
│       └── tables.js       # Tri des tables
└── assets/
    └── *.png               # Images
```

## Installation

```bash
cd EXPORT
pip install -r requirements.txt
```

## Lancement

```bash
python main.py
# ou
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Accès : http://localhost:8000/dashboard

## Endpoints API

| Endpoint | Description |
|----------|-------------|
| `GET /api/filters/sites` | Liste des sites |
| `GET /api/filters/options` | Options dynamiques (types/moments) |
| `GET /api/sessions/general` | Vue générale |
| `GET /api/sessions/site-details` | Détails par site |
| `GET /api/sessions/error-analysis` | Analyse des erreurs |

## Paramètres de filtre (query params)

- `sites` : Sites séparés par virgule
- `date_debut` : Date de début (YYYY-MM-DD)
- `date_fin` : Date de fin (YYYY-MM-DD)
- `error_types` : Types d'erreur séparés par virgule
- `moments` : Moments d'erreur séparés par virgule

## Configuration DB

Variables d'environnement (ou modifier `db.py`) :
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

## Intégration Laravel

Les fichiers sont conçus pour être facilement intégrables :

1. **API** : Adapter les routers Python en contrôleurs Laravel
2. **Frontend** : Les fichiers JS/CSS sont séparés et peuvent être inclus directement
3. **Templates** : Les templates Jinja2 peuvent être convertis en Blade

Le HTML et le JS sont séparés pour faciliter l'intégration.
