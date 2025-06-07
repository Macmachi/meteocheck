# 🌦 MeteoCheck - Bot Météo Telegram Avancé

### 📌 Description
MeteoCheck est un bot Telegram météorologique complet qui surveille et analyse les conditions météorologiques 24h/7j en utilisant l'[API open-meteo.com](https://open-meteo.com/en/docs).

#### 🔄 Fonctionnalités de surveillance automatique
* 🌡️ Enregistre les données météo horaires : température, précipitations, 🌬️ vitesse du vent, ☀️ indice UV, 📊 pression atmosphérique et 💧 humidité
* 🕰️ Surveillance continue avec vérifications météo chaque minute
* 🚨 Système d'alertes intelligent pour conditions météo extrêmes :
  * Températures dangereuses (> 35°C ou < -10°C)
  * Fortes précipitations (> 15mm avec probabilité > 80%)
  * Vents violents (> 60 km/h)
  * Indice UV élevé (> 8)
  * **🌪️ Détection de bombes météorologiques** avec calculs scientifiques adaptés à la latitude

#### 📊 Commandes d'analyse météo
* **Données actuelles :**
  * `/start` - Démarrage et liste complète des commandes
  * `/weather` - Dernières données météo enregistrées
  * `/forecast` - Prévisions détaillées pour les 6 prochaines heures

* **Résumés statistiques :**
  * `/month` - Résumé complet du mois dernier
  * `/year` - Résumé de l'année en cours
  * `/all` - Résumé de toutes les données historiques
  * `/daterange YYYY-MM-DD YYYY-MM-DD` - Résumé personnalisé entre deux dates
  * `/sunshine` - Calculs d'ensoleillement mensuel basés sur l'indice UV

#### 📈 Visualisations graphiques avancées
* **Graphiques temporels :**
  * `/graph <métrique> [jours]` - Graphiques détaillés avec moyennes mobiles
    * Métriques : `temperature`, `rain`, `wind`, `pressure`, `uv`, `humidity`
    * Exemple : `/graph temperature 30` (30 derniers jours)
  * `/forecastgraph` - Graphique des prévisions 24h avec codes couleur météo style MeteoSuisse

* **Analyses comparatives :**
  * `/heatmap [année|all]` - Calendriers thermiques style GitHub
    * `/heatmap 2024` - Année spécifique
    * `/heatmap all` - Vue multi-années
  * `/yearcompare [métrique]` - Comparaison inter-annuelle avec tendances
  * `/sunshinecompare` - Analyse multi-années de l'ensoleillement
  * `/top10 <métrique>` - Classement des valeurs extrêmes

#### 🎯 Exemples d'utilisation
/graph rain 7          # Précipitations des 7 derniers jours
/heatmap all           # Calendrier thermique toutes années
/yearcompare wind      # Comparaison des vents par année
/sunshinecompare       # Évolution ensoleillement multi-années
/top10 temperature     # Records de température
/daterange 2024-06-01 2024-08-31  # Analyse été 2024

### 🔗 Liens utiles
* 🔗 [Configurateur API pour votre ville](https://open-meteo.com/en/docs)

### 🆕 Nouvelles fonctionnalités majeures v2025
* [✅] **Système de cache intelligent** - Optimisation des appels API avec cache 5 minutes
* [✅] **Graphiques météo avancés** avec palettes MeteoSuisse et moyennes mobiles
* [✅] **Calendriers thermiques multi-années** style GitHub avec vue comparative
* [✅] **Détection scientifique de bombes météorologiques** avec seuils ajustés par latitude
* [✅] **Système de comparaison inter-annuelle** avec analyse de tendances
* [✅] **Analyse d'ensoleillement multi-années** avec calculs astronomiques précis
* [✅] **Top 10 des records** avec horodatage et classification
* [✅] **Résumés de période personnalisables** avec commande `/daterange`
* [✅] **Interface moderne** avec emojis et codes couleur météorologiques
* [✅] **Gestion intelligente des notifications** - Anti-spam pour records prévus

### 🚀 Fonctionnalités techniques avancées
* **Architecture asynchrone moderne** avec aiogram 3.x et aiohttp pour performances optimales
* **Système de cache API** avec validation temporelle automatique
* **Visualisations matplotlib/seaborn** avec palettes MeteoSuisse authentiques
* **Gestion robuste des erreurs** avec retry automatique et logging détaillé
* **Calculs astronomiques** pour lever/coucher du soleil selon latitude exacte
* **Palettes de couleurs météorologiques** conformes aux standards internationaux
* **Moyennes mobiles** et analyses de tendances avec régression linéaire
* **Formatage temporel intelligent** adaptatif selon la période d'analyse
* **Anti-spam intelligent** pour les alertes de records avec confiance adaptative

### 🎨 Améliorations visuelles v2025
* **Graphiques style GitHub** avec zones saisonnières colorées
* **Gradient de couleurs MeteoSuisse** pour précipitations (bruine → extrême)
* **Légendes interactives** avec emojis saisonniers et indicateurs d'intensité
* **Axes temporels intelligents** - Formatage automatique selon la période
* **Zones d'intérêt visuelles** - Mise en évidence de la période actuelle
* **Statistiques enrichies** - Tendances, moyennes et comparaisons automatiques

### ⚠️ Note importante sur les données temporelles
Le script enregistre toutes les données en UTC pour garantir la cohérence. L'API Open-Meteo est interrogée en GMT (UTC), et les affichages utilisateur sont automatiquement convertis en heure locale Europe/Berlin. Le stockage UTC assure l'intégrité des comparaisons historiques et des calculs astronomiques précis.

### 🧠 Intelligence artificielle intégrée
* **Détection de patterns météo** - Identification automatique de conditions extrêmes
* **Prédiction de confiance** - Score adaptatif selon la proximité temporelle
* **Alertes contextuelles** - Messages personnalisés selon l'intensité et l'urgence
* **Optimisation adaptative** - Seuils d'alerte ajustés selon l'historique local

### 🪱 Logs et débogage
* Logs détaillés dans `log_meteocheck.log` avec horodatage UTC
* Gestion d'erreurs multi-niveaux avec retry automatique
* Nettoyage automatique du CSV au démarrage avec validation des données
* Monitoring des performances API avec statistiques de cache
