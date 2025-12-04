# IE Charge Dashboard

Dashboard de monitoring des bornes de charge électrique.  
Architecture découplée **FastAPI + HTMX** : chaque onglet charge ses données au clic (lazy load).

## Structure

```
ie-charge/
├── main.py                 # Point d'entrée FastAPI
├── db.py                   # Connexion MySQL + helpers
├── routers/
│   ├── defauts.py          # /api/defauts-actifs
│   ├── alertes.py          # /api/alertes
│   ├── sessions.py         # /api/sessions/stats
│   └── kpis.py             # /api/kpi/suspicious, /api/kpi/multi-attempts
├── templates/
│   ├── index.html          # Layout principal + onglets
│   └── partials/           # Fragments HTML (retournés par HTMX)
│       ├── defauts_actifs.html
│       ├── alertes.html
│       ├── kpi_card.html
│       └── sessions_stats.html
├── static/
│   └── style.css           # Styles
└── requirements.txt
```

## Installation

```bash
# Créer un environnement virtuel
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou: venv\Scripts\activate  # Windows

# Installer les dépendances
pip install -r requirements.txt
```

## Configuration

Variables d'environnement (optionnel, sinon valeurs par défaut) :

```bash
export DB_HOST=162.19.251.55
export DB_PORT=3306
export DB_USER=nidec
export DB_PASSWORD=MaV38f5xsGQp83
export DB_NAME=Charges
```

## Lancement

```bash
# Mode développement (avec hot reload)
python main.py

# Ou directement avec uvicorn
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Ouvrir http://localhost:8000

## Comment ça marche

1. **Page principale** (`/`) : charge le layout avec les filtres et les onglets
2. **Lazy loading** : chaque section KPI et chaque onglet fait un appel HTMX au clic
3. **Fragments HTML** : l'API retourne des morceaux de HTML (pas du JSON) directement injectés dans la page
4. **Filtres** : quand on applique les filtres, un événement `filtersChanged` déclenche le rechargement de toutes les sections

## Ajouter un nouvel onglet

1. Créer un router dans `routers/mon_onglet.py`
2. Créer le template partiel dans `templates/partials/mon_onglet.html`
3. Inclure le router dans `main.py`
4. Ajouter le mapping dans le JS de `index.html`

## Migration depuis Streamlit

Ce dashboard remplace progressivement l'application Streamlit.  
Avantages :
- **Pas de chargement global** : seules les données nécessaires sont chargées
- **Cache côté serveur** : peut être partagé entre utilisateurs
- **Performance** : pas de rechargement complet à chaque interaction
