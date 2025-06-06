# 🌦 MeteoCheck - Bot Météo Telegram Avancée

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
  * `/forecastgraph` - Graphique des prévisions 24h avec codes couleur méteo

* **Analyses comparatives :**
  * `/heatmap [année|all]` - Calendriers thermiques style GitHub
    * `/heatmap 2024` - Année spécifique
    * `/heatmap all` - Vue multi-années
  * `/yearcompare [métrique]` - Comparaison inter-annuelle avec tendances
  * `/top10 <métrique>` - Classement des valeurs extrêmes

#### 🎯 Exemples d'utilisation
/graph rain 7          # Précipitations des 7 derniers jours
/heatmap all           # Calendrier thermique toutes années
/yearcompare wind      # Comparaison des vents par année
/top10 temperature     # Records de température
/daterange 2024-06-01 2024-08-31  # Analyse été 2024

### 🔗 Liens utiles
* 🔗 [Configurateur API pour votre ville](https://open-meteo.com/en/docs)

### 🆕 Nouvelles fonctionnalités majeures
* [✅] **Graphiques météo avancés** avec palettes de couleurs modernes et moyennes mobiles
* [✅] **Calendriers thermiques** style GitHub avec vue multi-années
* [✅] **Détection scientifique de bombes météorologiques** avec seuils ajustés par latitude
* [✅] **Système de comparaison inter-annuelle** avec analyse de tendances
* [✅] **Top 10 des records** avec horodatage précis
* [✅] **Résumés de période personnalisables** avec commande `/daterange`
* [✅] **Calculs d'ensoleillement astronomiques** précis selon la géolocalisation
* [✅] **Interface moderne** avec emojis et codes couleur météorologiques

### 🚀 Fonctionnalités techniques avancées
* **Architecture asynchrone** avec aiogram 3.x pour performances optimales
* **Visualisations matplotlib/seaborn** avec styles modernes
* **Gestion robuste des erreurs** et logging détaillé
* **Calculs astronomiques** pour lever/coucher du soleil selon latitude
* **Palettes de couleurs météorologiques** conformes aux standards MeteoSuisse
* **Moyennes mobiles** et analyses de tendances
* **Formatage temporel intelligent** adaptatif selon la période

### ⚠️ Note importante sur les données temporelles
Le script enregistre toutes les données en UTC. L'API Open-Meteo est interrogée en GMT (UTC), garantissant une cohérence temporelle. Les affichages pour l'utilisateur sont automatiquement convertis en heure locale Europe/Berlin, mais le stockage reste en UTC pour assurer l'intégrité des comparaisons historiques et des calculs astronomiques.

### 🪱 Logs et débogage
* Logs détaillés dans `log_meteocheck.log`
* Gestion d'erreurs avec retry automatique
* Nettoyage automatique du CSV au démarrage

---

**🎯 MeteoCheck combine surveillance météo en temps réel, analyses statistiques poussées et visualisations modernes pour offrir une expérience météorologique complète via Telegram.**
