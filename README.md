# 🌦 MeteoCheck - Bot Météo Telegram Avancé

### 📌 Description
MeteoCheck est un bot Telegram météorologique complet qui surveille, analyse et prédit les conditions météorologiques 24h/7j en utilisant l'[API open-meteo.com]. Il intègre des systèmes d'alertes intelligents et des visualisations de données avancées pour fournir une analyse météo de niveau expert.

---

### 🔄 Fonctionnalités de Surveillance Automatique
* 🌡️ **Enregistrement des données météo horaires :** température, précipitations, 🌬️ vitesse du vent, ☀️ indice UV, 📊 pression atmosphérique et 💧 humidité.
* 🕰️ **Surveillance continue** avec vérifications météo chaque minute.
* 🚨 **Système d'alertes intelligent pour conditions météo extrêmes basées sur des seuils prédéfinis :**
  *   Températures dangereuses : **> 35°C** ou **< -10°C**
  *   Fortes précipitations : **> 15mm** (avec probabilité > 80%)
  *   Vents violents : **> 60 km/h**
  *   Indice UV élevé : **> 8**
* 🏆 **Analyse de Record Historique :** Lorsqu'une alerte de seuil est déclenchée, le bot vérifie si la valeur constitue également un record annuel ou absolu (voir section anti-spam ci-dessous).
* **🌪️ Détection de bombes météorologiques** avec calculs scientifiques adaptés à la latitude.
* ⛈️ **Suivi avancé des orages** avec alertes évolutives et rappels de mise à l'abri.

---

### 🧠 Gestion Intelligente des Alertes (Anti-Spam)

Le bot intègre des mécanismes sophistiqués pour éviter le spam tout en fournissant des informations critiques et contextuelles.

#### 🏆 Anti-Spam pour les Notifications de Record

Lorsqu'une valeur météo dépasse un seuil d'alerte extrême (listé ci-dessus), le bot lance une seconde analyse pour déterminer si cette valeur constitue également un record historique. C'est pour **ces alertes spécifiques aux records** que le système de validation suivant s'applique pour décider d'envoyer ou non une notification :

1.  **🥇 Vérification du Record :** La valeur doit d'abord **dépasser un record existant** dans la base de données (annuel ou absolu). C'est la condition de base pour même considérer une notification de record.

2.  **🧠 Évaluation de la Confiance :** Si un record potentiel est identifié, le bot lui attribue un **score de confiance** (ex: 90%, 75%) basé sur son éloignement dans le temps. Une notification n'est envoyée que si la confiance est jugée suffisante (généralement > 60-70%).

3.  **⏱️ Contrôle de Fréquence Adaptatif :** Pour éviter les notifications répétitives pour des valeurs très proches, le bot vérifie s'il a déjà envoyé une alerte similaire récemment. La "récence" est définie par des paliers temporels intelligents :
    *   **Record imminent (< 3h) :** Mises à jour possibles toutes les **30 minutes**.
    *   **Record proche (< 12h) :** Mises à jour possibles toutes les **2 heures**.
    *   **Record lointain (> 24h) :** Mises à jour possibles toutes les **12 heures**.

4.  **📉 Gestion des Annulations :** Si un record potentiel qui avait été notifié **disparaît** des prévisions ultérieures, le bot envoie une **notification de mise à jour** pour informer que le risque est écarté. Cela évite les fausses alertes et donne une vision dynamique de la situation.

#### ⛈️ Suivi d'Orages violents avec Paliers de Notification
Pour les phénomènes orageux violents, le bot active un mode de suivi qui gère l'événement du début à la fin.

*   **Gestion de l'Événement :** Le système envoie des alertes distinctes pour une **NOUVELLE** alerte, une **MISE À JOUR** (changement d'heure ou d'intensité) et une **FIN D'ALERTE**.
*   **Cooldown Anti-Flapping :** Pour éviter les alertes contradictoires (ex: alerte puis annulation immédiate), un cooldown adaptatif est activé, empêchant une nouvelle notification pendant :
    *   **15 minutes** si l'orage est à moins d'1h.
    *   **30 minutes** si l'orage est à moins de 3h.
    *   **1 heure** si l'orage est plus lointain.
*   **Paliers de Rappel "Mise à l'Abri" :** Des notifications programmées sont envoyées à des moments clés avant l'impact :
    *   **T-2h :** Rappel de planification.
    *   **T-1h :** Rappel de préparation.
    *   **T-20min :** Alerte d'urgence.
    *   **T-5min :** Alerte d'impact imminent.

---

### 📊 Commandes d'Analyse Météo
*   **/start** - Démarrage et liste complète des commandes.
*   **/weather** - Météo actuelle (prévisions) et dernière mesure enregistrée.
*   **/forecast** - Prévisions détaillées pour les 6 prochaines heures.
*   **/month** - Résumé complet du mois dernier.
*   **/year** - Résumé de l'année en cours.
*   **/all** - Résumé de toutes les données historiques.
*   **/daterange YYYY-MM-DD YYYY-MM-DD** - Résumé personnalisé entre deux dates.
*   **/top10 <métrique>** - Classement des 10 valeurs les plus extrêmes.

---

### 📈 Visualisations Graphiques Avancées
*   **/graph <métrique> [jours]** - **2 graphiques** (courbe + barres) avec moyennes mobiles et style adaptatif.
*   **/forecastgraph** - Graphique des prévisions 24h avec codes couleur météo style MeteoSuisse.
*   **/heatmap [année|all]** - Calendriers thermiques style GitHub (vue annuelle ou multi-années).
*   **/yearcompare [métrique]** - Comparaison inter-annuelle avec tendances et mise en évidence de la période actuelle.
*   **/sunshine** - Graphique en barres de l'ensoleillement mensuel par année.
*   **/sunshinelist** - Liste texte de l'ensoleillement mensuel estimé.

---

### 🚀 Fonctionnalités Techniques
*   **Architecture asynchrone** avec `aiogram 3.x` et `aiohttp` pour des performances optimales.
*   **Système de cache API intelligent** avec validation temporelle pour réduire la latence et les appels.
*   **Visualisations `matplotlib/seaborn`** avec palettes de couleurs professionnelles (MeteoSuisse, GitHub).
*   **Gestion robuste des erreurs** avec `try/except`, logging détaillé et tentatives d'envoi multiples.
*   **Calculs astronomiques** pour l'ensoleillement, adaptés à la latitude/longitude.
*   **Filtrage automatique des données incomplètes** pour assurer la pertinence statistique des analyses (`/yearcompare`, `/top10`).
*   **Formatage temporel intelligent** des axes de graphiques en fonction de la période analysée.

---

### ⚠️ Note sur les Données Temporelles
Toutes les données sont enregistrées en **UTC** pour garantir la cohérence et l'intégrité des comparaisons historiques. Les affichages pour l'utilisateur sont automatiquement convertis en heure locale (`Europe/Berlin`) pour un confort d'utilisation optimal.

---

### 🧠 Intelligence Artificielle et Logique Adaptative
*   **Détection de Patterns Météo :** Identification automatique de conditions extrêmes (bombes météo, orages).
*   **Prédiction de Confiance :** Score adaptatif pour les records potentiels basé sur la proximité temporelle.
*   **Alertes Contextuelles :** Messages personnalisés selon l'intensité, le type (nouveau, mise à jour, annulation) et l'urgence.
*   **Optimisation Adaptative :** Seuils d'alerte et de notification ajustés dynamiquement pour éviter la fatigue informationnelle.
*   **Gestion du "Flapping" :** Cooldowns intelligents pour stabiliser les alertes lors de prévisions fluctuantes.
