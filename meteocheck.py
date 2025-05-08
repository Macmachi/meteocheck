'''
*
* PROJET : MeteoCheck
* AUTEUR : Arnaud R.
* VERSIONS : v1.7.0
* NOTES : None
*
'''
import os
import pandas as pd
import asyncio
import datetime
import aiofiles
import aiohttp # Utilisé pour les requêtes HTTP asynchrones
import sys
import traceback
import configparser
import json
import pytz # Pour la gestion des fuseaux horaires
import logging # Ajout pour un meilleur logging

# Imports spécifiques à aiogram 3.x
from aiogram import Bot, Dispatcher, types, Router
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext # Si vous prévoyez d'utiliser FSM plus tard
from aiogram.filters import Command
from aiogram.exceptions import TelegramAPIError # Pour la gestion des erreurs de l'API Telegram

# Configuration and setup
script_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_dir)
config_path = os.path.join(script_dir, 'config.ini')

config = configparser.ConfigParser()
config.read(config_path)

TOKEN_TELEGRAM = config['KEYS']['TELEGRAM_BOT_TOKEN']
VILLE = config['LOCATION']['VILLE']
LATITUDE = config['LOCATION']['LATITUDE']
LONGITUDE = config['LOCATION']['LONGITUDE']

weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=temperature_2m,precipitation_probability,precipitation,pressure_msl,windspeed_10m,uv_index,relativehumidity_2m&timezone=GMT&forecast_days=2&past_days=2&models=best_match&timeformat=unixtime"

# Initialisation du Bot et du Dispatcher pour aiogram 3.x
bot = Bot(token=TOKEN_TELEGRAM)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router() # Création d'un routeur principal pour les handlers

csv_filename = "weather_data.csv"

# Initialize CSV file if not exists
if not os.path.exists(csv_filename):
    df_initial = pd.DataFrame(columns=['time', 'temperature_2m', 'precipitation_probability', 'precipitation', 'pressure_msl', 'windspeed_10m', 'uv_index', 'relativehumidity_2m'])
    df_initial.to_csv(csv_filename, index=False)

# Logging functions
def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    error_message = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    try:
        # Essayer d'obtenir la boucle d'événements en cours pour y soumettre la tâche de log
        loop = asyncio.get_running_loop()
        loop.create_task(log_message(f"Uncaught exception: {error_message}"))
    except RuntimeError:  # Aucune boucle d'événements en cours (par exemple, si l'erreur se produit très tôt)
        # Exécuter de manière synchrone comme fallback (crée une nouvelle boucle temporaire)
        asyncio.run(log_message(f"Uncaught exception (no running loop): {error_message}"))

sys.excepthook = log_uncaught_exceptions

async def log_message(message: str):
    async with aiofiles.open("log_meteocheck.log", mode='a', encoding='utf-8') as f:
        await f.write(f"{datetime.datetime.now(pytz.UTC)} - {message}\n")

def clean_csv_file():
    try:
        df = pd.read_csv(csv_filename)
        if df.empty:
            print("Fichier CSV vide, rien à nettoyer.")
            return

        correct_columns = ['time', 'temperature_2m', 'precipitation_probability', 'precipitation', 'pressure_msl', 'windspeed_10m', 'uv_index', 'relativehumidity_2m']
        
        df = df.reindex(columns=correct_columns)
        
        # S'assurer que la colonne 'time' existe avant de la manipuler
        if 'time' in df.columns:
            df['time'] = df['time'].astype(str).str.replace(' ', 'T', regex=False).str.replace('+00:00', 'Z', regex=False)
            df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
            
            invalid_entries = df['time'].isna().sum()
            df = df.dropna(subset=['time'])
            if invalid_entries > 0:
                print(f"{invalid_entries} entrées invalides supprimées du CSV après conversion de date.")
            
            df = df.sort_values('time')
            df['time'] = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            print("Colonne 'time' manquante dans le CSV. Impossible de nettoyer les dates.")

        df.to_csv(csv_filename, index=False)
        print("Fichier CSV nettoyé avec succès.")
    except pd.errors.EmptyDataError:
        print("Fichier CSV vide ou mal formaté lors du nettoyage.")
    except Exception as e:
        print(f"Erreur lors du nettoyage du fichier CSV : {str(e)}")
        # Log l'erreur pour un débogage plus facile
        asyncio.run(log_message(f"Erreur critique lors du nettoyage du CSV: {str(e)}"))


# Alert tracking
sent_alerts = {
    'temperature': None,
    'precipitation': None,
    'windspeed': None,
    'uv_index': None,
    'pressure_msl': None
}

# Alert and message sending functions
async def schedule_jobs():
    """Tâche planifiée pour vérifier la météo périodiquement."""
    while True:
        await check_weather()
        await asyncio.sleep(60)

async def send_alert(message_text, row=None, alert_column=None):
    """Envoie une alerte à tous les utilisateurs enregistrés."""
    if row is not None and alert_column is not None:
        # La fonction check_records envoie elle-même des alertes si un record est battu.
        # Pour éviter les alertes en double, on ne fait pas d'await ici directement.
        # check_records appellera send_alert pour le message de record.
        await check_records(row, alert_column)

    try:
        # Utilisation de aiofiles pour lire chat_ids.json de manière asynchrone
        if os.path.exists('chat_ids.json'):
            async with aiofiles.open('chat_ids.json', 'r') as file:
                content = await file.read()
                chats = json.loads(content)
        else:
            chats = []

        send_tasks = [send_message_with_retry(chat_id, message_text) for chat_id in chats]
        await asyncio.gather(*send_tasks)
        await log_message(f"Sent alert: {message_text}")
    except FileNotFoundError:
        await log_message("Erreur dans send_alert: chat_ids.json non trouvé.")
    except json.JSONDecodeError:
        await log_message("Erreur dans send_alert: Impossible de décoder chat_ids.json.")
    except Exception as e:
        await log_message(f"Error in send_alert: {str(e)}")

async def send_message_with_retry(chat_id, message_text, max_retries=3):
    """Tente d'envoyer un message avec plusieurs essais en cas d'erreur API."""
    for attempt in range(max_retries):
        try:
            await bot.send_message(chat_id=chat_id, text=message_text)
            return
        except TelegramAPIError as e: # Utilisation de l'exception importée
            await log_message(f"Tentative {attempt + 1} d'envoi au chat {chat_id} échouée: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1) # Attendre avant de réessayer
            else:
                await log_message(f"Échec final de l'envoi du message au chat {chat_id} après {max_retries} tentatives : {e}")
                # Ne pas print directement, mais logger l'échec
        except Exception as e_general: # Capturer d'autres exceptions potentielles
            await log_message(f"Erreur inattendue lors de l'envoi au chat {chat_id} (tentative {attempt+1}): {e_general}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
            else:
                await log_message(f"Échec final (erreur inattendue) de l'envoi au chat {chat_id} après {max_retries} tentatives: {e_general}")


async def send_summary_base(chat_id, df_period, period_name, ville_name):
    """Fonction de base pour générer et envoyer des résumés."""
    if df_period.empty:
        await log_message(f"Aucune donnée disponible pour {period_name}")
        await bot.send_message(chat_id, f"Pas de données disponibles pour {period_name}.")
        return
    
    summary = generate_summary(df_period)
    await log_message(f"Résumé pour {period_name} généré avec succès")
    
    message_text = f"Résumé {period_name} pour {ville_name}:\n\n{summary}"
    await bot.send_message(chat_id, message_text)
    await log_message(f"Résumé {period_name} envoyé avec succès à chat_id: {chat_id}")

async def send_month_summary(chat_id):
    try:
        await log_message(f"Début de send_month_summary pour chat_id: {chat_id}")
        df = pd.read_csv(csv_filename)
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce').dropna()
        
        now = pd.Timestamp.now(tz='UTC')
        # Mois dernier : du premier jour du mois précédent au premier jour du mois actuel (exclus)
        first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        first_day_last_month = (first_day_current_month - pd.DateOffset(months=1))
        
        df_last_month = df[(df['time'] >= first_day_last_month) & (df['time'] < first_day_current_month)]
        await log_message(f"Données filtrées pour le mois dernier ({first_day_last_month.strftime('%Y-%m')}): {len(df_last_month)} entrées")
        
        await send_summary_base(chat_id, df_last_month, "du mois dernier", VILLE)
    
    except pd.errors.EmptyDataError:
        await handle_summary_error(chat_id, "Le fichier CSV est vide ou mal formaté.")
    except FileNotFoundError:
        await handle_summary_error(chat_id, f"Le fichier {csv_filename} n'a pas été trouvé.")
    except aiohttp.ClientError as e: # Erreur réseau lors de l'envoi du message
        await handle_summary_error(chat_id, f"Erreur de connexion lors de l'envoi du message: {str(e)}", is_network_error=True)
    except Exception as e:
        await handle_summary_error(chat_id, f"Erreur inattendue dans send_month_summary: {str(e)}")

async def send_year_summary(chat_id):
    try:
        await log_message(f"Début de send_year_summary pour chat_id: {chat_id}")
        df = pd.read_csv(csv_filename)
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce').dropna()
        
        now = pd.Timestamp.now(tz='UTC')
        current_year = now.year
        df_current_year = df[(df['time'].dt.year == current_year) & (df['time'] < now)]
        await log_message(f"Données filtrées pour l'année en cours ({current_year}): {len(df_current_year)} entrées")

        await send_summary_base(chat_id, df_current_year, "de l'année en cours", VILLE)

    except pd.errors.EmptyDataError:
        await handle_summary_error(chat_id, "Le fichier CSV est vide ou mal formaté.")
    except FileNotFoundError:
        await handle_summary_error(chat_id, f"Le fichier {csv_filename} n'a pas été trouvé.")
    except aiohttp.ClientError as e:
        await handle_summary_error(chat_id, f"Erreur de connexion lors de l'envoi du message: {str(e)}", is_network_error=True)
    except Exception as e:
        await handle_summary_error(chat_id, f"Erreur inattendue dans send_year_summary: {str(e)}")

async def send_all_summary(chat_id):
    try:
        await log_message(f"Début de send_all_summary pour chat_id: {chat_id}")
        df = pd.read_csv(csv_filename)
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce').dropna()
        
        await log_message(f"Données lues pour le résumé complet: {len(df)} entrées")
        await send_summary_base(chat_id, df, "de toutes les données météo", VILLE)

    except pd.errors.EmptyDataError:
        await handle_summary_error(chat_id, "Le fichier CSV est vide ou mal formaté.")
    except FileNotFoundError:
        await handle_summary_error(chat_id, f"Le fichier {csv_filename} n'a pas été trouvé.")
    except aiohttp.ClientError as e:
        await handle_summary_error(chat_id, f"Erreur de connexion lors de l'envoi du message: {str(e)}", is_network_error=True)
    except Exception as e:
        await handle_summary_error(chat_id, f"Erreur inattendue dans send_all_summary: {str(e)}")

async def handle_summary_error(chat_id, error_msg, is_network_error=False):
    """Gère les erreurs communes pour les fonctions de résumé."""
    await log_message(error_msg)
    if is_network_error:
        await bot.send_message(chat_id, "Erreur de connexion. Veuillez réessayer plus tard.")
    else:
        await bot.send_message(chat_id, f"Erreur : {error_msg}")


async def get_weather_data():
    """Récupère les données météo de l'API et met à jour le CSV."""
    try:
        now = pd.Timestamp.now(tz='UTC').floor('h')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            async with session.get(weather_url) as resp:
                resp.raise_for_status() # Lève une exception pour les codes HTTP 4xx/5xx
                data = await resp.json()
                
                if 'hourly' not in data or not isinstance(data['hourly'], dict):
                    await log_message("Format de données API inattendu: 'hourly' manquant ou incorrect.")
                    return pd.DataFrame(), pd.DataFrame()

                columns = ['time', 'temperature_2m', 'precipitation_probability', 'precipitation', 'pressure_msl', 'windspeed_10m', 'uv_index', 'relativehumidity_2m']
                
                # Vérifier que toutes les colonnes attendues sont présentes dans les données API
                for col in columns:
                    if col not in data['hourly']:
                        await log_message(f"Colonne API manquante: '{col}' dans data['hourly']")
                        # Retourner des DataFrames vides si une colonne essentielle manque
                        if col == 'time': return pd.DataFrame(), pd.DataFrame()
                        # Pour les autres colonnes, on pourrait initialiser avec des NaN si nécessaire
                        # mais pour la cohérence, retourner vide est plus sûr.
                        return pd.DataFrame(), pd.DataFrame()

                df_api = pd.DataFrame({col: data['hourly'][col] for col in columns})
                df_api['time'] = pd.to_datetime(df_api['time'], unit='s', utc=True)
                
                # Lecture du CSV existant
                if os.path.exists(csv_filename) and os.path.getsize(csv_filename) > 0:
                    try:
                        df_existing = pd.read_csv(csv_filename)
                        df_existing['time'] = pd.to_datetime(df_existing['time'], utc=True, errors='coerce')
                        df_existing.dropna(subset=['time'], inplace=True) # S'assurer que les dates sont valides
                    except pd.errors.EmptyDataError:
                         df_existing = pd.DataFrame(columns=columns)
                         df_existing['time'] = pd.to_datetime(df_existing['time'], utc=True) # Assurer le dtype
                else:
                    df_existing = pd.DataFrame(columns=columns)
                    df_existing['time'] = pd.to_datetime(df_existing['time'], utc=True) # Assurer le dtype

                # Données des dernières 24h de l'API pour combler les manques
                twenty_four_hours_ago = now - pd.Timedelta(hours=24)
                last_twenty_four_hours_df_api = df_api[(df_api['time'] >= twenty_four_hours_ago) & (df_api['time'] < now)].copy() # .copy() pour éviter SettingWithCopyWarning

                if not last_twenty_four_hours_df_api.empty:
                    missing_data = last_twenty_four_hours_df_api[~last_twenty_four_hours_df_api['time'].isin(df_existing['time'])].copy()
                    if not missing_data.empty:
                        missing_data['time'] = missing_data['time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        # S'assurer que le header n'est écrit que si le fichier est vide ou n'existe pas
                        header_needed = not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0
                        missing_data.to_csv(csv_filename, mode='a', header=header_needed, index=False)
                        await log_message(f"Enregistrement de {len(missing_data)} nouvelles données manquantes dans le CSV.")
                
                # Prévisions pour les prochaines heures
                seven_hours_later = now + pd.Timedelta(hours=7)
                next_seven_hours_df = df_api[(df_api['time'] > now) & (df_api['time'] <= seven_hours_later)].copy()
                
                twenty_four_hours_later = now + pd.Timedelta(hours=24)
                next_twenty_four_hours_df = df_api[(df_api['time'] > now) & (df_api['time'] <= twenty_four_hours_later)].copy()
                
                return next_seven_hours_df, next_twenty_four_hours_df

    except aiohttp.ClientError as e: # Erreurs réseau spécifiques à aiohttp
        await log_message(f"Erreur réseau dans get_weather_data: {str(e)}")
    except json.JSONDecodeError as e:
        await log_message(f"Erreur de décodage JSON dans get_weather_data: {str(e)}")
    except KeyError as e:
        await log_message(f"Clé manquante dans les données API (get_weather_data): {str(e)}")
    except Exception as e:
        await log_message(f"Erreur inattendue dans get_weather_data: {str(e)}\n{traceback.format_exc()}")
    return pd.DataFrame(), pd.DataFrame()


async def check_weather():
    """Vérifie la météo et envoie des alertes si nécessaire."""
    await log_message("Fonction check_weather exécutée")
    try:
        df_next_seven_hours, df_next_twenty_four_hours = await get_weather_data()
        
        if df_next_seven_hours.empty and df_next_twenty_four_hours.empty:
            await log_message("Aucune donnée obtenue de get_weather_data dans check_weather. Prochaine vérification dans 1 minute.")
            return

        # Alertes pour les 7 prochaines heures
        for _, row in df_next_seven_hours.iterrows():
            # S'assurer que row est bien une Series Pandas et non un tuple si iterrows est mal utilisé
            if not isinstance(row, pd.Series): 
                await log_message(f"Format de ligne inattendu dans df_next_seven_hours: {type(row)}")
                continue

            time_local = row['time'].tz_convert('Europe/Berlin')
            # Comparaison de date uniquement pour sent_alerts
            alert_date_key = time_local.date()

            if row['temperature_2m'] > 35 or row['temperature_2m'] < -10:
                if sent_alerts['temperature'] != alert_date_key:
                    emoji = "🔥" if row['temperature_2m'] > 35 else "❄️"
                    await send_alert(f"{emoji} Alerte météo : Température prévue de {row['temperature_2m']:.1f}°C à {time_local.strftime('%H:%M')} à {VILLE}.", row, 'temperature_2m')
                    sent_alerts['temperature'] = alert_date_key
            
            if row['precipitation_probability'] > 80 and row['precipitation'] > 15:
                if sent_alerts['precipitation'] != alert_date_key:
                    await send_alert(f"🌧️ Alerte météo : Fortes pluies prévues de {row['precipitation']:.1f}mm à {time_local.strftime('%H:%M')} à {VILLE}.", row, 'precipitation')
                    sent_alerts['precipitation'] = alert_date_key
            
            if row['windspeed_10m'] > 60: # km/h
                if sent_alerts['windspeed'] != alert_date_key:
                    emoji = "🌪️" if row['windspeed_10m'] > 75 else "💨"
                    wind_type = "tempétueux" if row['windspeed_10m'] > 75 else "fort"
                    await send_alert(f"{emoji} Alerte météo : Vent {wind_type} prévu de {row['windspeed_10m']:.1f}km/h à {time_local.strftime('%H:%M')} à {VILLE}.", row, 'windspeed_10m')
                    sent_alerts['windspeed'] = alert_date_key
            
            if row['uv_index'] > 8:
                if sent_alerts['uv_index'] != alert_date_key:
                    await send_alert(f"☀️ Alerte météo : Index UV prévu de {row['uv_index']:.1f} à {time_local.strftime('%H:%M')} à {VILLE}.", row, 'uv_index')
                    sent_alerts['uv_index'] = alert_date_key

        # Alerte de bombe météorologique pour les 24 prochaines heures
        if len(df_next_twenty_four_hours) >= 24: # S'assurer d'avoir assez de données pour comparer t0 et t+23h
            # iloc[0] est la première heure de prévision, iloc[23] est la 24ème heure de prévision
            pressure_drop = df_next_twenty_four_hours['pressure_msl'].iloc[0] - df_next_twenty_four_hours['pressure_msl'].iloc[23]
            if pressure_drop >= 20: # hPa
                time_event_start_local = df_next_twenty_four_hours['time'].iloc[0].tz_convert('Europe/Berlin')
                alert_date_key_pressure = time_event_start_local.date()
                if sent_alerts['pressure_msl'] != alert_date_key_pressure:
                    # Utiliser la première ligne du DataFrame des 24h pour check_records
                    await send_alert(f"🌪️ Alerte météo : Risque de bombe météorologique détecté. Baisse de pression prévue de {round(pressure_drop, 2)} hPa sur 24 heures à partir de {time_event_start_local.strftime('%H:%M le %d/%m')}.", df_next_twenty_four_hours.iloc[0], 'pressure_msl')
                    sent_alerts['pressure_msl'] = alert_date_key_pressure
    
    except KeyError as e:
        await log_message(f"Erreur de clé dans check_weather (probablement une colonne manquante dans le DataFrame): {str(e)}")
    except Exception as e:
        await log_message(f"Erreur inattendue dans check_weather: {str(e)}\n{traceback.format_exc()}")


async def check_records(row_alert, alert_column):
    """Vérifie si une valeur entrante bat un record annuel."""
    try:
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await log_message("Fichier CSV vide ou inexistant, impossible de vérifier les records.")
            return

        df = pd.read_csv(csv_filename)
        if df.empty:
            await log_message("Fichier CSV lu mais vide, impossible de vérifier les records.")
            return

        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)

        current_year = pd.Timestamp.now(tz='UTC').year
        df_current_year = df[df['time'].dt.year == current_year].copy() # .copy() pour éviter les warnings

        if df_current_year.empty:
            await log_message(f"Aucune donnée pour l'année en cours ({current_year}), impossible de vérifier les records pour {alert_column}.")
            # Si c'est la première donnée de l'année, elle établit le record initial
            time_local = row_alert['time'].tz_convert('Europe/Berlin')
            await send_alert(f"🏆 Info météo : Première donnée de l'année pour {alert_column} : {row_alert[alert_column]} à {time_local.strftime('%H:%M')}.")
            return

        # S'assurer que la colonne d'alerte existe et est numérique
        if alert_column not in df_current_year.columns:
            await log_message(f"Colonne {alert_column} non trouvée dans le CSV pour vérifier les records.")
            return
        if not pd.api.types.is_numeric_dtype(df_current_year[alert_column]):
            await log_message(f"Colonne {alert_column} n'est pas numérique dans le CSV pour vérifier les records.")
            return


        # Vérification du record max
        max_value_year = df_current_year[alert_column].max()
        if pd.notna(row_alert[alert_column]) and row_alert[alert_column] > max_value_year:
            time_local = row_alert['time'].tz_convert('Europe/Berlin')
            # Note: send_alert est appelé ici, ce qui peut créer une récursion si check_records est appelé depuis send_alert sans condition.
            # La conception actuelle de send_alert semble gérer cela en ne rappelant pas check_records si row/alert_column sont None.
            # Pour un message de record, on ne passe pas row/alert_column pour éviter la boucle.
            await send_alert(f"🏆 Alerte météo : Nouveau record annuel (max) pour {alert_column} : {row_alert[alert_column]} (précédent: {max_value_year}) à {time_local.strftime('%H:%M')}.")

        # Vérification du record min (spécifiquement pour la température)
        if alert_column == 'temperature_2m':
            min_value_year = df_current_year[alert_column].min()
            if pd.notna(row_alert[alert_column]) and row_alert[alert_column] < min_value_year:
                time_local = row_alert['time'].tz_convert('Europe/Berlin')
                await send_alert(f"🏆 Alerte météo : Nouveau record annuel (min) pour {alert_column} : {row_alert[alert_column]} (précédent: {min_value_year}) à {time_local.strftime('%H:%M')}.")

    except pd.errors.EmptyDataError:
        await log_message("Fichier CSV vide lors de la vérification des records.")
    except FileNotFoundError:
        await log_message(f"Fichier {csv_filename} non trouvé lors de la vérification des records.")
    except KeyError as e:
        await log_message(f"Erreur de clé dans check_records (colonne manquante?): {str(e)}")
    except Exception as e:
        await log_message(f"Erreur inattendue dans check_records: {str(e)}\n{traceback.format_exc()}")


def calculate_sunshine_hours(df_calc):
    """Calcule les heures d'ensoleillement estimées à partir d'un DataFrame."""
    # Créer une copie pour éviter SettingWithCopyWarning si df_calc est une slice
    df = df_calc.copy()
    
    # Assurer que 'time' est datetime et localisé
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)

    if df['time'].dt.tz is None:
         df['time'] = df['time'].dt.tz_localize('UTC')

    df['local_time'] = df['time'].dt.tz_convert('Europe/Berlin')

    # Définition des heures de jour (approximatif, pourrait être amélioré avec des bibliothèques d'éphémérides)
    # Valeurs par défaut
    df['is_daytime'] = False

    # Été (juin-août)
    summer_mask = df['local_time'].dt.month.isin([6, 7, 8])
    df.loc[summer_mask, 'is_daytime'] = (df.loc[summer_mask, 'local_time'].dt.hour >= 5) & (df.loc[summer_mask, 'local_time'].dt.hour < 22)
    
    # Printemps (mars-mai) et Automne (septembre-novembre)
    midseason_mask = df['local_time'].dt.month.isin([3, 4, 5, 9, 10, 11])
    df.loc[midseason_mask, 'is_daytime'] = (df.loc[midseason_mask, 'local_time'].dt.hour >= 6) & (df.loc[midseason_mask, 'local_time'].dt.hour < 21)
    
    # Hiver (décembre-février)
    winter_mask = df['local_time'].dt.month.isin([12, 1, 2])
    df.loc[winter_mask, 'is_daytime'] = (df.loc[winter_mask, 'local_time'].dt.hour >= 8) & (df.loc[winter_mask, 'local_time'].dt.hour < 17)
    
    # Ensoleillé si c'est le jour ET UV > 2 (seuil arbitraire pour "ensoleillé")
    # S'assurer que uv_index est numérique et gérer les NaN
    if 'uv_index' in df.columns and pd.api.types.is_numeric_dtype(df['uv_index']):
        df['is_sunny'] = df['is_daytime'] & (df['uv_index'].fillna(0) > 2)
        sunshine_hours = df['is_sunny'].sum() # Chaque ligne représente une heure
    else:
        sunshine_hours = 0 # Pas de données UV ou non numérique

    return sunshine_hours


def calculate_monthly_sunshine(df_calc):
    """Calcule les heures d'ensoleillement par mois."""
    df = df_calc.copy()
    if df.empty:
        return pd.Series(dtype=float)

    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
    df.dropna(subset=['time'], inplace=True)
    if df['time'].dt.tz is None:
         df['time'] = df['time'].dt.tz_localize('UTC')
    
    # Grouper par mois et appliquer le calcul d'ensoleillement
    # apply peut être lent sur de gros dataframes, mais pour des données mensuelles, c'est acceptable.
    monthly_sunshine = df.groupby(pd.Grouper(key='time', freq='MS')).apply(calculate_sunshine_hours) # MS for Month Start
    
    # Formatter l'index pour la lisibilité
    if not monthly_sunshine.empty:
        monthly_sunshine.index = monthly_sunshine.index.strftime('%Y-%m')
    
    return monthly_sunshine


def generate_summary(df_summary):
    """Génère un texte de résumé météo à partir d'un DataFrame."""
    df = df_summary.copy() # Travailler sur une copie
    if df.empty:
        return "Aucune donnée à résumer."

    # Assurer que 'time' est datetime et UTC
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
    df.dropna(subset=['time'], inplace=True)
    if df['time'].dt.tz is None:
        df['time'] = df['time'].dt.tz_localize('UTC')

    # Calculs statistiques (s'assurer que les colonnes existent et sont numériques)
    # Fonction utilitaire pour obtenir une statistique ou une valeur par défaut
    def get_stat(df_stat, column, operation):
        if column not in df_stat.columns or not pd.api.types.is_numeric_dtype(df_stat[column]) or df_stat[column].isnull().all():
            return pd.NA, pd.NaT if operation in ['idxmax', 'idxmin'] else pd.NA
        
        if operation == 'max': return df_stat[column].max()
        if operation == 'min': return df_stat[column].min()
        if operation == 'idxmax': return df_stat[column].idxmax() if not df_stat[column].empty else pd.NaT
        if operation == 'idxmin': return df_stat[column].idxmin() if not df_stat[column].empty else pd.NaT
        if operation == 'mean': return df_stat[column].mean()
        if operation == 'sum': return df_stat[column].sum()
        if operation == 'nunique_rainy': # Cas spécial pour les jours de pluie
            df_stat['day_for_rain'] = df_stat['time'].dt.date
            daily_precip = df_stat.groupby('day_for_rain')[column].sum()
            return daily_precip[daily_precip > 0.1].count()
        return pd.NA

    # Précipitations journalières
    if 'precipitation' in df.columns and pd.api.types.is_numeric_dtype(df['precipitation']):
        df['day'] = df['time'].dt.date
        # Calculer la somme des précipitations par jour, gérer les NaN en les remplaçant par 0 pour la somme
        df['daily_precipitation'] = df.groupby('day')['precipitation'].transform(lambda x: x.fillna(0).sum())
    else:
        df['daily_precipitation'] = 0 # Colonne par défaut si 'precipitation' manque

    sunshine_hours = calculate_sunshine_hours(df)

    max_temp, idx_hot_day = get_stat(df, 'temperature_2m', 'max'), get_stat(df, 'temperature_2m', 'idxmax')
    min_temp, idx_cold_day = get_stat(df, 'temperature_2m', 'min'), get_stat(df, 'temperature_2m', 'idxmin')
    max_precipitation, idx_rain_day = get_stat(df, 'daily_precipitation', 'max'), get_stat(df, 'daily_precipitation', 'idxmax')
    max_uv_index, idx_uv_day = get_stat(df, 'uv_index', 'max'), get_stat(df, 'uv_index', 'idxmax')
    max_wind_speed, idx_wind_day = get_stat(df, 'windspeed_10m', 'max'), get_stat(df, 'windspeed_10m', 'idxmax')
    avg_temp = get_stat(df, 'temperature_2m', 'mean')
    total_precipitation = get_stat(df, 'precipitation', 'sum')
    max_humidity, idx_humid_day = get_stat(df, 'relativehumidity_2m', 'max'), get_stat(df, 'relativehumidity_2m', 'idxmax')
    min_humidity, idx_dry_day = get_stat(df, 'relativehumidity_2m', 'min'), get_stat(df, 'relativehumidity_2m', 'idxmin')
    rainy_days = get_stat(df, 'precipitation', 'nunique_rainy')


    # Formatter les dates pour l'affichage (convertir en 'Europe/Berlin')
    def format_date_from_idx(idx, df_ref):
        if pd.isna(idx) or idx not in df_ref.index: return "N/A"
        return df_ref.loc[idx, 'time'].tz_convert('Europe/Berlin').strftime("%Y-%m-%d")

    hot_day_berlin = format_date_from_idx(idx_hot_day, df)
    cold_day_berlin = format_date_from_idx(idx_cold_day, df)
    # Pour rain_day, l'index vient de 'daily_precipitation' qui peut avoir un index différent après groupby.
    # On utilise l'index original du DataFrame où 'daily_precipitation' a été assigné.
    rain_day_berlin = format_date_from_idx(idx_rain_day, df) if pd.notna(idx_rain_day) else "N/A"

    uv_day_berlin = format_date_from_idx(idx_uv_day, df)
    wind_day_berlin = format_date_from_idx(idx_wind_day, df)
    humid_day_berlin = format_date_from_idx(idx_humid_day, df)
    dry_day_berlin = format_date_from_idx(idx_dry_day, df)

    # Créer le résumé en gérant les valeurs N/A
    summary_parts = [
        f"🌡️ Jour le plus chaud: {hot_day_berlin} ({max_temp:.1f}°C)" if pd.notna(max_temp) else "🌡️ Jour le plus chaud: N/A",
        f"❄️ Jour le plus froid: {cold_day_berlin} ({min_temp:.1f}°C)" if pd.notna(min_temp) else "❄️ Jour le plus froid: N/A",
        f"🌧️ Jour le plus pluvieux: {rain_day_berlin} ({max_precipitation:.1f}mm)" if pd.notna(max_precipitation) else "🌧️ Jour le plus pluvieux: N/A",
        f"☀️ Jour avec l'index UV le plus élevé: {uv_day_berlin} (index {max_uv_index:.1f})" if pd.notna(max_uv_index) else "☀️ Index UV max: N/A",
        f"💨 Jour le plus venteux: {wind_day_berlin} ({max_wind_speed:.1f} km/h)" if pd.notna(max_wind_speed) else "💨 Vent max: N/A",
        f"🌂 Nombre de jours de pluie (>0.1mm): {rainy_days}" if pd.notna(rainy_days) else "🌂 Jours de pluie: N/A",
        f"🌡️ Température moyenne: {avg_temp:.1f}°C" if pd.notna(avg_temp) else "🌡️ Temp. moyenne: N/A",
        f"💧 Précipitations totales: {total_precipitation:.1f}mm" if pd.notna(total_precipitation) else "💧 Précip. totales: N/A",
        f"💦 Jour le plus humide: {humid_day_berlin} ({max_humidity:.1f}%)" if pd.notna(max_humidity) else "💦 Humidité max: N/A",
        f"🏜️ Jour le plus sec: {dry_day_berlin} ({min_humidity:.1f}%)" if pd.notna(min_humidity) else "🏜️ Humidité min: N/A",
        f"☀️ Heures d'ensoleillement estimées: {sunshine_hours:.1f} heures" if pd.notna(sunshine_hours) else "☀️ Ensoleillement: N/A"
    ]
    return "\n".join(summary_parts)

# --- Définition des Handlers avec le Router ---
@router.message(Command("start"))
async def start_command(message: types.Message):
    chat_id = message.chat.id
    is_new_user = True
    
    chats = []
    # Utilisation de aiofiles pour lire/écrire chat_ids.json
    if os.path.exists('chat_ids.json'):
        try:
            async with aiofiles.open('chat_ids.json', 'r', encoding='utf-8') as file:
                content = await file.read()
                chats = json.loads(content)
            if chat_id in chats:
                is_new_user = False
            else:
                chats.append(chat_id)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            await log_message(f"Erreur lecture chat_ids.json pour start: {e}. Initialisation avec nouveau user.")
            chats = [chat_id] # repartir d'une liste fraîche
    else:
        chats = [chat_id]
    
    try:
        async with aiofiles.open('chat_ids.json', 'w', encoding='utf-8') as file:
            await file.write(json.dumps(chats))
    except Exception as e:
        await log_message(f"Erreur écriture chat_ids.json pour start: {e}")

    welcome_ville = VILLE if VILLE else "votre localité" # Fallback si VILLE n'est pas défini
    if is_new_user:
        welcome_message = (
            f"Bienvenue sur le bot météo de {welcome_ville}! 🌤️\n\n"
            "Voici les commandes disponibles :\n"
            "/weather - Obtenir les dernières informations météo enregistrées\n"
            "/forecast - Voir les prévisions pour les prochaines heures\n"
            "/sunshine - Voir le résumé mensuel de l'ensoleillement\n"
            "/month - Obtenir le résumé météo du mois dernier\n"
            "/year - Obtenir le résumé météo de l'année en cours\n"
            "/all - Obtenir le résumé météo de toutes les données disponibles\n\n"
            f"N'hésitez pas à utiliser ces commandes pour rester informé sur la météo à {welcome_ville}!"
        )
        await message.reply(welcome_message)
    else:
        welcome_back_message = (
            "Vous avez déjà lancé le bot !\n\n"
            "Mais voici un rappel des commandes disponibles :\n"
            "/weather, /forecast, /sunshine, /month, /year, /all\n\n"
            "Quelle information météo souhaitez-vous obtenir aujourd'hui?"
        )
        await message.reply(welcome_back_message)

@router.message(Command("weather"))
async def get_latest_info_command(message: types.Message):
    try:
        await log_message("Début de get_latest_info_command")
        
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await log_message("Fichier CSV inexistant ou vide pour /weather.")
            await message.reply("Aucune donnée météo disponible pour le moment.")
            return

        df = pd.read_csv(csv_filename)
        await log_message("CSV lu avec succès pour /weather")
        
        if df.empty:
            await log_message("Le DataFrame est vide pour /weather")
            await message.reply("Aucune donnée disponible.")
        else:
            # S'assurer que la colonne 'time' est bien en datetime pour le tri avant de prendre la dernière
            df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
            df.dropna(subset=['time'], inplace=True)
            df.sort_values(by='time', inplace=True)

            if df.empty: # Peut devenir vide après dropna
                 await message.reply("Aucune donnée valide disponible après nettoyage.")
                 return

            latest_info = df.iloc[-1].to_dict()
            # La date est déjà en UTC, la convertir pour affichage
            time_display = pd.to_datetime(latest_info['time'], utc=True).tz_convert('Europe/Berlin').strftime("%Y-%m-%d %H:%M:%S")
            
            response_parts = [f"🌡️ Météo la plus récente enregistrée à {VILLE} :\n"]
            response_parts.append(f"📅 {time_display}\n")
            response_parts.append(f"🌡️ Température: {latest_info.get('temperature_2m', 'N/A')}°C")
            response_parts.append(f"🌧️ Probabilité de pluie: {latest_info.get('precipitation_probability', 'N/A')}%")
            response_parts.append(f"💧 Précipitations: {latest_info.get('precipitation', 'N/A')}mm")
            response_parts.append(f"💨 Vent: {latest_info.get('windspeed_10m', 'N/A')}km/h")
            response_parts.append(f"☀️ Indice UV: {latest_info.get('uv_index', 'N/A')}")
            response_parts.append(f"🎈 Pression: {latest_info.get('pressure_msl', 'N/A')} hPa") # Changé emoji
            response_parts.append(f"💦 Humidité: {latest_info.get('relativehumidity_2m', 'N/A')}%")
            
            await log_message("Réponse /weather préparée, tentative d'envoi")
            await message.reply("\n".join(response_parts))
            await log_message("Réponse /weather envoyée avec succès")

    except pd.errors.EmptyDataError:
        await log_message("Fichier CSV vide pour /weather (EmptyDataError).")
        await message.reply("Aucune donnée météo disponible (fichier vide).")
    except FileNotFoundError:
        await log_message(f"Fichier {csv_filename} non trouvé pour /weather.")
        await message.reply("Source de données météo non trouvée.")
    except Exception as e:
        await log_message(f"Error in get_latest_info_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply(f"Erreur lors de l'obtention des informations : {str(e)}")


@router.message(Command("forecast"))
async def get_forecast_command(message: types.Message): # Renommé pour clarté
    try:
        df_next_seven_hours, _ = await get_weather_data() # Ignorer df_next_twenty_four_hours ici
        if df_next_seven_hours.empty:
            await message.reply("Aucune donnée de prévision disponible pour le moment.")
            return
        
        # Prendre les 6 prochaines heures de prévision
        forecast_df = df_next_seven_hours.head(6)
        response = f"🔮 Prévisions météo pour {VILLE} (prochaines heures):\n\n"
        
        for _, row in forecast_df.iterrows():
            time_local = row['time'].tz_convert('Europe/Berlin').strftime("%H:%M")
            temp = row.get('temperature_2m', float('nan'))
            precip = row.get('precipitation', float('nan'))
            precip_prob = row.get('precipitation_probability', float('nan'))
            wind = row.get('windspeed_10m', float('nan'))
            uv = row.get('uv_index', float('nan'))
            humidity = row.get('relativehumidity_2m', float('nan'))
            
            temp_emoji = "🥵" if temp > 30 else "🥶" if temp < 10 else "🌡️"
            precip_emoji = "🌧️" if precip > 0.1 else "☀️" # Seuil pour pluie
            wind_emoji = "🌬️" if wind > 30 else "🍃" # Seuil pour vent fort
            
            response += f"⏰ {time_local}:\n"
            response += f"{temp_emoji} {temp:.1f}°C | "
            response += f"{precip_emoji} {precip:.1f}mm ({precip_prob:.0f}%) | "
            response += f"{wind_emoji} {wind:.1f}km/h | "
            response += f"☀️ UV: {uv:.1f} | "
            response += f"💦 {humidity:.0f}%\n\n"
        
        await message.reply(response)
    except Exception as e:
        await log_message(f"Error in get_forecast_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply("Erreur lors de l'obtention des prévisions.")


@router.message(Command("sunshine"))
async def get_sunshine_summary_command(message: types.Message): # Renommé pour clarté
    try:
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await message.reply("Aucune donnée disponible pour calculer l'ensoleillement.")
            return
            
        df = pd.read_csv(csv_filename)
        if df.empty:
            await message.reply("Aucune donnée disponible pour calculer l'ensoleillement (fichier vide).")
            return

        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True) # Important après conversion

        if df.empty: # Peut devenir vide après dropna
            await message.reply("Aucune donnée temporelle valide pour calculer l'ensoleillement.")
            return

        monthly_sunshine = calculate_monthly_sunshine(df) # Attendre le résultat
        
        if monthly_sunshine.empty:
            await message.reply(f"Pas encore de données d'ensoleillement calculées pour {VILLE}.")
            return

        # Trier par date (l'index est déjà YYYY-MM, donc tri alphabétique fonctionne)
        monthly_sunshine = monthly_sunshine.sort_index()
        
        response = f"☀️ Résumé mensuel de l'ensoleillement estimé pour {VILLE} :\n\n"
        current_month_year_str = pd.Timestamp.now(tz='Europe/Berlin').strftime('%Y-%m')
        
        for date_str, hours in monthly_sunshine.items():
            # Convertir YYYY-MM en nom de mois et année pour affichage
            try:
                month_display = pd.to_datetime(date_str + "-01").strftime('%B %Y')
            except ValueError:
                month_display = date_str # Fallback si le format est inattendu
            
            is_current = date_str == current_month_year_str
            month_marker = "📍 " if is_current else "📅 "
            response += f"{month_marker}{month_display}: {hours:.1f} heures\n"
        
        await message.reply(response)
        
    except pd.errors.EmptyDataError:
        await message.reply("Le fichier de données est vide, impossible de calculer l'ensoleillement.")
    except FileNotFoundError:
        await message.reply("Fichier de données non trouvé pour calculer l'ensoleillement.")
    except Exception as e:
        await log_message(f"Error in get_sunshine_summary_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply("Erreur lors de l'obtention du résumé de l'ensoleillement.")


@router.message(Command("month"))
async def get_month_summary_command(message: types.Message): # Renommé
    await send_month_summary(message.chat.id)

@router.message(Command("year"))
async def get_year_summary_command(message: types.Message): # Renommé
    await send_year_summary(message.chat.id)

@router.message(Command("all"))
async def get_all_summary_command(message: types.Message): # Renommé
    await send_all_summary(message.chat.id)


# --- Fonction principale pour démarrer le bot ---
async def main():
    # Inclure le routeur principal dans le dispatcher
    dp.include_router(router)

    # Lancer la tâche de fond pour vérifier la météo
    # Elle utilisera l'instance globale `bot` pour envoyer des messages
    asyncio.create_task(schedule_jobs())
    
    # Démarrer le polling du bot
    # skip_updates=True est utile pour ignorer les mises à jour reçues pendant que le bot était hors ligne
    await log_message("Bot démarré et prêt à recevoir les commandes.")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    # Configurer le logging de base pour voir les messages d'aiogram et les vôtres
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Nettoyer le CSV au démarrage
    clean_csv_file()
    
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        asyncio.run(log_message("Bot arrêté manuellement.")) # Log l'arrêt
        logging.info("Bot arrêté.")
    except Exception as e:
        # Log l'erreur fatale qui a empêché le bot de tourner
        asyncio.run(log_message(f"Erreur fatale au niveau principal du bot: {str(e)}\n{traceback.format_exc()}"))
        logging.critical(f"Erreur fatale: {e}", exc_info=True)