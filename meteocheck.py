'''
*
* PROJET : MeteoCheck
* AUTEUR : Rymentz
* VERSIONS : v2.0.1
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
import matplotlib
matplotlib.use('Agg')  # Backend non-interactif pour serveurs
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import numpy as np
import io
import calendar
import math
from datetime import datetime as dt

# Configuration style moderne pour les graphiques
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# Configuration des polices pour éviter les warnings emoji
import warnings
import matplotlib.font_manager as fm

# Désactiver les warnings spécifiques aux glyphes manquants de manière plus robuste
warnings.filterwarnings('ignore', category=UserWarning, module='matplotlib.font_manager')
warnings.filterwarnings('ignore', category=UserWarning, message='.*Glyph.*missing.*')
warnings.filterwarnings('ignore', category=UserWarning, message='.*\\\\N.*missing.*')

# Function pour remplacer les emojis par des alternatives textuelles pour matplotlib
def replace_emojis_for_matplotlib(text):
    """Remplace les emojis par des alternatives textuelles pour éviter les warnings matplotlib."""
    emoji_replacements = {
        # Emojis météo de base
        '📅': '[Cal]',
        '🌧️': '[Rain]',
        '📊': '[Chart]',
        '🟣': '[Dot]',
        '☀️': '[Sun]',
        '❄️': '[Snow]',
        '🔥': '[Fire]',
        '🌡️': '[Temp]',
        '💨': '[Wind]',
        '🎈': '[Press]',
        '💦': '[Humid]',
        '🌦️': '[Weather]',
        '🏆': '[Trophy]',
        '🔮': '[Crystal]',
        '📈': '[Up]',
        '📉': '[Down]',
        '📍': '[Pin]',
        '🌤️': '[PartCloud]',
        '🌸': '[Spring]',
        '🍂': '[Autumn]',
        '🏖️': '[Weekend]',
        '🌅': '[Day]',
        '💧': '[Drop]',
        '🏜️': '[Dry]',
        '🔴': '[Red]',
        '🟦': '[Blue]',
        '💙': '[LBlue]',
        '⚠️': '[Warning]',
        '💀': '[Danger]',
        '🌪️': '[Storm]',
        '🥵': '[Hot]',
        '🥶': '[Cold]',
        '🌬️': '[WindStrong]',
        '🍃': '[WindLight]',
        '🌊': '[Wave]',
        
        # Emojis d'alerte et time
        '🚨': '[Alert]',
        '⏰': '[Clock]',
        '⏱️': '[Timer]',
        '🤖': '[Bot]',
        '🔧': '[Tool]',
        '🎯': '[Target]',
        '🌂': '[Umbrella]',
        '🌈': '[Rainbow]',
        
        # Formes et couleurs
        '🔵': '[BlueDot]',
        '🟨': '[Yellow]',
        '🟩': '[Green]',
        '🟪': '[Purple2]',
        '⚫': '[Black]',
        '⚪': '[White]',
        '🔶': '[OrangeDiam]',
        '🔷': '[BlueDiam]',
        '🔸': '[SmallOrangeDiam]',
        '🔹': '[SmallBlueDiam]',
        '🔺': '[RedTriUp]',
        '🔻': '[RedTriDown]',
        '💎': '[Diamond]',
        '🔱': '[Trident]',
        '⭕': '[Circle]',
        '❌': '[X]',
        '❓': '[Question]',
        '❗': '[Exclamation]',
        '💯': '[100]',
        
        # Contrôles et tech
        '🆔': '[ID]',
        '🆘': '[SOS]',
        '🅰️': '[A]',
        '🅱️': '[B]',
        '🆎': '[AB]',
        '🅾️': '[O]',
        '🔀': '[Shuffle]',
        '🔁': '[Repeat]',
        '🔂': '[RepeatOne]',
        '⏭️': '[NextTrack]',
        '⏯️': '[PlayPause]',
        '⏹️': '[Stop]',
        '⏺️': '[Record]',
        '🎦': '[Cinema]',
        '🔅': '[DimBright]',
        '🔆': '[BrightUp]',
        '📶': '[Signal]',
        '📳': '[Vibrate]',
        '📴': '[PhoneOff]',
        
        # Symboles divers
        '♀️': '[Female]',
        '♂️': '[Male]',
        '⚕️': '[Medical]',
        '♻️': '[Recycle]',
        '⚜️': '[FleurDeLis]',
        '🔰': '[Beginner]',
        '📛': '[NameBadge]',
        '❇️': '[Sparkle]',
        '✳️': '[EightSpoke]',
        '❎': '[CrossMark]',
        '🌀': '[Cyclone]',
        '💤': '[Sleep]',
        '♨️': '[HotSprings]',
        '💫': '[Dizzy]',
        '🌟': '[GlowStar]',
        '⭐': '[Star]',
        '🌙': '[CrescentMoon]',
        '☄️': '[Comet]',
        '🌍': '[EarthAfrica]',
        '🌎': '[EarthAmericas]',
        '🌏': '[EarthAsia]',
        '🌋': '[Volcano]',
        '🗻': '[MountFuji]',
        '🏔️': '[SnowMountain]',
        '🌄': '[SunriseMountain]',
        '🌆': '[Cityscape]',
        '🌇': '[Sunset]',
        '🌉': '[BridgeNight]',
        '🌌': '[MilkyWay]',
        '🎆': '[Fireworks]',
        '🎇': '[Sparkler]',
        '🌃': '[NightStars]',
        '🏙️': '[Cityscape2]',
        '🌠': '[ShootingStar]',
        
        # Emojis de direction
        '↗️': '[UpRight]',
        '↘️': '[DownRight]',
        '➡️': '[Right]',
        
        # Emojis de nature/saisons
        '☁️': '[Cloud]',
        '🌥': '[CloudSun]',
        '⛅': '[PartlyCloudy]',
        
        # Coeurs et sentiments
        '💛': '[YellowHeart]',
        '💜': '[PurpleHeart]',
        '💚': '[GreenHeart]',
        '❤️': '[RedHeart]'
    }
    
    result = str(text)
    for emoji, replacement in emoji_replacements.items():
        result = result.replace(emoji, replacement)
    return result

# Trouver une police système qui supporte mieux les caractères Unicode
available_fonts = [f.name for f in fm.fontManager.ttflist]
emoji_compatible_fonts = ['Segoe UI Emoji', 'Apple Color Emoji', 'Noto Color Emoji', 'Arial Unicode MS', 'DejaVu Sans', 'Liberation Sans']
selected_font = 'sans-serif'  # Fallback par défaut

for font in emoji_compatible_fonts:
    if font in available_fonts:
        selected_font = font
        break

plt.rcParams.update({
    'figure.facecolor': '#f8f9fa',
    'axes.facecolor': '#ffffff',
    'axes.edgecolor': '#dee2e6',
    'axes.linewidth': 0.8,
    'axes.grid': True,
    'grid.color': '#e9ecef',
    'grid.alpha': 0.6,
    'font.family': ['sans-serif'],
    'font.sans-serif': [selected_font, 'DejaVu Sans', 'Liberation Sans', 'Arial'],
    'font.size': 10,
    'axes.titlesize': 14,
    'axes.labelsize': 11,
    'xtick.labelsize': 9,
    'ytick.labelsize': 9,
    'legend.fontsize': 10,
    'figure.titlesize': 16,
    'axes.unicode_minus': False,  # Éviter les problèmes avec les caractères Unicode
    'text.usetex': False  # Désactiver LaTeX qui peut causer des problèmes avec Unicode
})

# Imports spécifiques à aiogram 3.x
from aiogram import Bot, Dispatcher, types, Router
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext # Si vous prévoyez d'utiliser FSM plus tard
from aiogram.filters import Command
from aiogram.exceptions import TelegramAPIError # Pour la gestion des erreurs de l'API Telegram
from aiogram.types import BufferedInputFile # Pour l'envoi de fichiers en mémoire

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

# ================================
# SYSTÈME DE CACHE PRÉVISIONS
# ================================
cached_forecast_data = {
    'df_seven_hours': pd.DataFrame(),
    'df_twenty_four_hours': pd.DataFrame(),
    'last_update': None,
    'cache_duration_minutes': 5  # Cache valide pendant 5 minutes
}

def is_cache_valid():
    """Vérifie si le cache des prévisions est encore valide."""
    if cached_forecast_data['last_update'] is None:
        return False
    
    now = pd.Timestamp.now(tz='UTC')
    time_since_update = (now - cached_forecast_data['last_update']).total_seconds() / 60
    return time_since_update < cached_forecast_data['cache_duration_minutes']

async def get_cached_forecast_data():
    """Retourne les données de prévision depuis le cache si valide, sinon depuis l'API."""
    if is_cache_valid():
        # Cache valide, retourner les données stockées
        return (cached_forecast_data['df_seven_hours'].copy(),
                cached_forecast_data['df_twenty_four_hours'].copy())
    else:
        # Cache expiré, appeler l'API et mettre à jour le cache
        await log_message("Cache des prévisions expiré, récupération depuis l'API...")
        df_seven, df_twenty_four = await get_weather_data()
        
        # Mettre à jour le cache
        cached_forecast_data['df_seven_hours'] = df_seven.copy()
        cached_forecast_data['df_twenty_four_hours'] = df_twenty_four.copy()
        cached_forecast_data['last_update'] = pd.Timestamp.now(tz='UTC')
        
        await log_message(f"Cache des prévisions mis à jour avec {len(df_seven)} + {len(df_twenty_four)} entrées")
        return df_seven, df_twenty_four

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
    'pressure_msl': None,
    'data_freshness': None  # Pour tracking des alertes de fraîcheur des données
}

# Advanced record tracking system
predicted_records = {
    'temperature_2m': {'max': {}, 'min': {}},
    'precipitation': {'max': {}},
    'windspeed_10m': {'max': {}},
    'pressure_msl': {'max': {}},
    'uv_index': {'max': {}}
}

# Structure: predicted_records[metric][type][record_id] = {
#   'value': float,
#   'time': datetime,
#   'confidence': float,
#   'first_detected': datetime,
#   'last_seen': datetime,
#   'notified': bool
# }

# History of sent record alerts (to avoid spam but allow important updates)
record_alert_history = []
# Structure: [{'type': str, 'metric': str, 'value': float, 'time': datetime, 'sent_at': datetime}]

# Alert and message sending functions
async def schedule_jobs():
    """Tâche planifiée pour vérifier la météo périodiquement."""
    while True:
        await check_weather()
        # Vérifier les changements dans les records prévus (notifications d'annulation)
        await check_predicted_record_changes()
        # Vérifier la fraîcheur des données (alerte si >24h)
        await check_data_freshness()
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
                        
                        # Vérifier les records sur les nouvelles données historiques réelles
                        for _, row in missing_data.iterrows():
                            # Reconvertir le temps en datetime pour check_records
                            row_copy = row.copy()
                            row_copy['time'] = pd.to_datetime(row_copy['time'], utc=True)
                            
                            # Vérifier les records pour les métriques principales
                            for metric in ['temperature_2m', 'precipitation', 'windspeed_10m', 'pressure_msl', 'uv_index']:
                                if metric in row_copy and pd.notna(row_copy[metric]):
                                    await check_records(row_copy, metric, is_forecast=False)
                
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

async def get_last_recorded_time():
    """Récupère la dernière heure enregistrée depuis les données historiques CSV."""
    try:
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            return None
        
        df = pd.read_csv(csv_filename)
        if df.empty:
            return None
        
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)
        
        if df.empty:
            return None
        
        # Obtenir la dernière entrée chronologique
        df_sorted = df.sort_values('time')
        last_entry = df_sorted.iloc[-1]
        last_data_time = last_entry['time']
        
        # Convertir en heure locale pour l'affichage
        last_data_local = last_data_time.tz_convert('Europe/Berlin')
        return last_data_local.strftime('%d/%m à %H:%M')
        
    except Exception as e:
        await log_message(f"Erreur dans get_last_recorded_time: {str(e)}")
        return None

async def check_weather():
    """Vérifie la météo et envoie des alertes si nécessaire."""
    await log_message("Fonction check_weather exécutée")
    try:
        # Utiliser le cache au lieu d'appeler directement l'API
        df_next_seven_hours, df_next_twenty_four_hours = await get_cached_forecast_data()
        
        if df_next_seven_hours.empty and df_next_twenty_four_hours.empty:
            await log_message("Aucune donnée obtenue de get_weather_data dans check_weather. Prochaine vérification dans 1 minute.")
            return

        # Récupérer la dernière heure enregistrée une seule fois
        last_recorded_time = await get_last_recorded_time()
        last_recorded_text = f"(dernière mesure: {last_recorded_time})" if last_recorded_time else "(aucune mesure historique)"

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
                    await send_alert(f"☀️ Alerte météo : Index UV prévu de {row['uv_index']:.1f} à {time_local.strftime('%H:%M')} à {VILLE} {last_recorded_text}.", row, 'uv_index')
                    sent_alerts['uv_index'] = alert_date_key

        # Détection de bombe météorologique améliorée pour les 24 prochaines heures
        if len(df_next_twenty_four_hours) >= 24:
            await detect_meteorological_bomb(df_next_twenty_four_hours)
    
    except KeyError as e:
        await log_message(f"Erreur de clé dans check_weather (probablement une colonne manquante dans le DataFrame): {str(e)}")
    except Exception as e:
        await log_message(f"Erreur inattendue dans check_weather: {str(e)}\n{traceback.format_exc()}")


async def detect_meteorological_bomb(df_forecast):
    """Détection avancée de bombe météorologique selon les critères scientifiques.
    
    Critères officiels :
    - Latitude 46°N (votre ville) : seuil ~22 hPa/24h (ajusté par sin(latitude))
    - Recherche de la plus forte baisse sur toute période de 24h consécutives
    - Conditions météo associées : vents forts + précipitations
    """
    global sent_alerts
    
    try:
        if len(df_forecast) < 24:
            return
            
        # Calcul du seuil ajusté par latitude (formule scientifique standard)
        CITY_LAT = float(LATITUDE)  # Latitude depuis config.ini
        base_threshold = 24.0  # hPa/24h à 60°N (référence)
        latitude_factor = np.sin(np.radians(60)) / np.sin(np.radians(CITY_LAT))
        adjusted_threshold = base_threshold * latitude_factor  # ~22.1 hPa pour 46°N
        
        # Recherche de la plus forte baisse sur toutes les fenêtres de 24h
        max_pressure_drop = 0
        bomb_start_idx = None
        bomb_end_idx = None
        
        for start_idx in range(len(df_forecast) - 23):
            end_idx = start_idx + 23
            pressure_start = df_forecast.iloc[start_idx]['pressure_msl']
            pressure_end = df_forecast.iloc[end_idx]['pressure_msl']
            pressure_drop = pressure_start - pressure_end
            
            if pressure_drop > max_pressure_drop:
                max_pressure_drop = pressure_drop
                bomb_start_idx = start_idx
                bomb_end_idx = end_idx
        
        # Vérification du seuil de bombe météorologique
        if max_pressure_drop >= adjusted_threshold and bomb_start_idx is not None:
            bomb_start_time = df_forecast.iloc[bomb_start_idx]['time']
            bomb_end_time = df_forecast.iloc[bomb_end_idx]['time']
            bomb_start_local = bomb_start_time.tz_convert('Europe/Berlin')
            bomb_end_local = bomb_end_time.tz_convert('Europe/Berlin')
            
            alert_date_key = bomb_start_local.date()
            
            if sent_alerts['pressure_msl'] != alert_date_key:
                # Analyse des conditions météo associées
                bomb_period = df_forecast.iloc[bomb_start_idx:bomb_end_idx+1]
                max_wind = bomb_period['windspeed_10m'].max()
                total_precip = bomb_period['precipitation'].sum()
                
                # Classification de l'intensité
                if max_pressure_drop >= adjusted_threshold * 1.5:  # ~33 hPa
                    intensity = "EXTRÊME"
                    emoji = "🌪️💀"
                elif max_pressure_drop >= adjusted_threshold * 1.2:  # ~27 hPa
                    intensity = "MAJEURE"
                    emoji = "🌪️⚠️"
                else:
                    intensity = "MODÉRÉE"
                    emoji = "🌪️"
                
                # Message d'alerte détaillé
                message = (
                    f"{emoji} BOMBE MÉTÉOROLOGIQUE {intensity} DÉTECTÉE !\n\n"
                    f"📉 Chute de pression : {max_pressure_drop:.1f} hPa en 24h\n"
                    f"🎯 Seuil scientifique (46°N) : {adjusted_threshold:.1f} hPa\n"
                    f"⏰ Période : {bomb_start_local.strftime('%d/%m %H:%M')} → {bomb_end_local.strftime('%d/%m %H:%M')}\n"
                    f"💨 Vent max prévu : {max_wind:.1f} km/h\n"
                    f"🌧️ Précipitations : {total_precip:.1f} mm\n\n"
                    f"⚠️ RISQUES : Vents violents, pluies torrentielles, conditions dangereuses"
                )
                
                # Envoyer l'alerte sans vérification de records (éviter récursion)
                await send_alert(message)
                sent_alerts['pressure_msl'] = alert_date_key
                
                await log_message(f"Bombe météorologique détectée : {max_pressure_drop:.1f} hPa en 24h (seuil: {adjusted_threshold:.1f})")
        
        # Alerte préventive si proche du seuil (80% du seuil)
        elif max_pressure_drop >= adjusted_threshold * 0.8:
            bomb_start_time = df_forecast.iloc[bomb_start_idx]['time']
            bomb_start_local = bomb_start_time.tz_convert('Europe/Berlin')
            alert_date_key = bomb_start_local.date()
            
            if sent_alerts['pressure_msl'] != alert_date_key:
                await send_alert(
                    f"⚠️ Surveillance météo : Forte baisse de pression prévue de {max_pressure_drop:.1f} hPa en 24h "
                    f"(proche du seuil de bombe météorologique: {adjusted_threshold:.1f} hPa). "
                    f"Début prévu : {bomb_start_local.strftime('%d/%m à %H:%M')}."
                )
                sent_alerts['pressure_msl'] = alert_date_key
                
    except Exception as e:
        await log_message(f"Erreur dans detect_meteorological_bomb: {str(e)}\n{traceback.format_exc()}")

async def check_data_freshness():
    """Vérifie si la dernière donnée historisée date de plus de 24h et envoie une alerte."""
    global sent_alerts
    
    try:
        # Vérifier que le fichier CSV existe et n'est pas vide
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await log_message("Fichier CSV inexistant ou vide pour vérification fraîcheur données.")
            return
        
        # Lire le CSV et obtenir la dernière entrée
        df = pd.read_csv(csv_filename)
        if df.empty:
            await log_message("CSV vide pour vérification fraîcheur données.")
            return
        
        # Convertir la colonne time et trier par date
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)
        
        if df.empty:
            await log_message("Aucune donnée temporelle valide pour vérification fraîcheur données.")
            return
        
        # Obtenir la dernière entrée chronologique
        df_sorted = df.sort_values('time')
        last_entry = df_sorted.iloc[-1]
        last_data_time = last_entry['time']
        
        # Heure actuelle
        now = pd.Timestamp.now(tz='UTC')
        
        # Calculer la différence en heures
        time_diff = now - last_data_time
        hours_since_last_data = time_diff.total_seconds() / 3600
        
        # Seuil de 24 heures
        alert_threshold_hours = 24
        
        # Convertir en heure locale pour l'affichage
        last_data_local = last_data_time.tz_convert('Europe/Berlin')
        last_data_str = last_data_local.strftime('%d/%m/%Y à %H:%M')
        
        # Vérifier si une alerte doit être envoyée
        current_date = now.date()
        
        if hours_since_last_data > alert_threshold_hours:
            # Éviter le spam : ne pas envoyer plus d'une alerte par jour
            if sent_alerts['data_freshness'] != current_date:
                # Formater le message d'alerte
                if hours_since_last_data > 48:
                    urgency_emoji = "🚨"
                    urgency_text = "CRITIQUE"
                    days_old = int(hours_since_last_data // 24)
                    time_description = f"{days_old} jour{'s' if days_old > 1 else ''}"
                elif hours_since_last_data > 36:
                    urgency_emoji = "⚠️"
                    urgency_text = "URGENT"
                    time_description = f"{hours_since_last_data:.1f} heures"
                else:
                    urgency_emoji = "⏰"
                    urgency_text = "ATTENTION"
                    time_description = f"{hours_since_last_data:.1f} heures"
                
                alert_message = (
                    f"{urgency_emoji} {urgency_text} - Données météo obsolètes !\n\n"
                    f"📊 Dernière donnée historisée : {last_data_str}\n"
                    f"⏱️ Âge des données : {time_description}\n"
                    f"🔴 Seuil dépassé : {alert_threshold_hours}h\n\n"
                    f"🤖 L'API OpenMeteo a peut-être changé ou le service est interrompu.\n"
                    f"🔧 Vérification et correction nécessaires."
                )
                
                # Envoyer l'alerte
                await send_alert(alert_message)
                sent_alerts['data_freshness'] = current_date
                
                await log_message(f"Alerte fraîcheur données envoyée: {hours_since_last_data:.1f}h depuis dernière donnée")
            else:
                await log_message(f"Données obsolètes ({hours_since_last_data:.1f}h) mais alerte déjà envoyée aujourd'hui")
        else:
            # Données fraîches : réinitialiser le tracking si nécessaire
            if sent_alerts['data_freshness'] is not None:
                await log_message(f"Données redevenues fraîches ({hours_since_last_data:.1f}h), réinitialisation tracking alertes")
                sent_alerts['data_freshness'] = None
            
    except pd.errors.EmptyDataError:
        await log_message("Fichier CSV vide lors de la vérification de fraîcheur.")
    except Exception as e:
        await log_message(f"Erreur dans check_data_freshness: {str(e)}\n{traceback.format_exc()}")


def calculate_confidence_score(forecast_time, current_time):
    """Calcule un score de confiance basé sur la proximité temporelle de la prévision.
    
    Args:
        forecast_time: Datetime de la prévision
        current_time: Datetime actuel
    
    Returns:
        float: Score entre 0 et 1 (1 = très fiable, 0 = peu fiable)
    """
    try:
        time_diff_hours = abs((forecast_time - current_time).total_seconds()) / 3600
        
        if time_diff_hours <= 3:
            return 1.0  # Très fiable (0-3h)
        elif time_diff_hours <= 12:
            return 0.8  # Fiable (3-12h)
        elif time_diff_hours <= 24:
            return 0.6  # Modérément fiable (12-24h)
        elif time_diff_hours <= 48:
            return 0.4  # Peu fiable (24-48h)
        else:
            return 0.2  # Très peu fiable (48h+)
    except:
        return 0.5  # Fallback

async def check_predicted_record_changes():
    """Vérifie les changements dans les records prévus et notifie si nécessaire."""
    global predicted_records
    
    try:
        current_time = pd.Timestamp.now(tz='UTC')
        changes_detected = []
        
        for metric in predicted_records:
            for record_type in predicted_records[metric]:
                records_to_remove = []
                
                for record_id, record_info in predicted_records[metric][record_type].items():
                    # Vérifier si le record est trop ancien sans mise à jour
                    time_since_last_seen = (current_time - record_info['last_seen']).total_seconds() / 3600
                    time_until_forecast = (record_info['time'] - current_time).total_seconds() / 3600
                    
                    # Délai d'expiration adaptatif selon la proximité du record
                    if time_until_forecast <= 6:
                        # Record très proche : expiration après 2h sans mise à jour
                        expiration_hours = 2
                    elif time_until_forecast <= 24:
                        # Record proche : expiration après 6h sans mise à jour
                        expiration_hours = 6
                    else:
                        # Record lointain : expiration après 12h sans mise à jour
                        expiration_hours = 12
                    
                    # Vérifier si le record a expiré (pas vu depuis trop longtemps)
                    if time_since_last_seen > expiration_hours:
                        # Record disparu - notification rapide d'annulation
                        if record_info['notified']:
                            changes_detected.append({
                                'type': 'expired',
                                'metric': metric,
                                'record_type': record_type,
                                'value': record_info['value'],
                                'time': record_info['time'],
                                'confidence': record_info['confidence'],
                                'missing_hours': time_since_last_seen
                            })
                        records_to_remove.append(record_id)
                    
                    # Nettoyage supplémentaire : supprimer les records passés
                    elif time_until_forecast < -2:  # Record passé depuis plus de 2h
                        records_to_remove.append(record_id)
                
                # Supprimer les records expirés
                for record_id in records_to_remove:
                    del predicted_records[metric][record_type][record_id]
        
        # Envoyer les notifications de changements
        for change in changes_detected:
            await notify_record_change(change)
            
    except Exception as e:
        await log_message(f"Erreur dans check_predicted_record_changes: {str(e)}")

async def notify_record_change(change_info):
    """Notifie un changement dans les records prévus."""
    try:
        metric_info = get_metric_info(change_info['metric'])
        time_local = change_info['time'].tz_convert('Europe/Berlin')
        
        if change_info['type'] == 'expired':
            confidence_text = f"{change_info['confidence']*100:.0f}%"
            missing_hours = change_info.get('missing_hours', 0)
            
            # Déterminer l'urgence du message selon la proximité
            time_until_forecast = (change_info['time'] - pd.Timestamp.now(tz='UTC')).total_seconds() / 3600
            
            if time_until_forecast <= 6:
                urgency_emoji = "⚠️"
                urgency_text = "URGENT - "
            elif time_until_forecast <= 24:
                urgency_emoji = "📉"
                urgency_text = ""
            else:
                urgency_emoji = "📋"
                urgency_text = ""
            
            message = (
                f"{urgency_emoji} {urgency_text}Mise à jour prévisions : Le record potentiel de {metric_info['name']} "
                f"({change_info['value']:.1f}{metric_info['unit']}) prévu pour "
                f"{time_local.strftime('%d/%m à %H:%M')} n'apparaît plus dans les prévisions depuis "
                f"{missing_hours:.1f}h. (Confiance était de {confidence_text})"
            )
            await send_alert(message)
            await log_message(f"Record potentiel expiré notifié: {change_info['metric']} {change_info['value']} (disparu depuis {missing_hours:.1f}h)")
            
    except Exception as e:
        await log_message(f"Erreur dans notify_record_change: {str(e)}")

async def update_predicted_records(row_alert, alert_column, is_forecast=True):
    """Met à jour le système de suivi des records prévus."""
    global predicted_records
    
    if not is_forecast:
        return  # On ne suit que les prévisions
    
    try:
        # Vérifier que la métrique est supportée
        if alert_column not in predicted_records:
            await log_message(f"Métrique {alert_column} non supportée pour le suivi des records prévus.")
            return
        
        current_time = pd.Timestamp.now(tz='UTC')
        forecast_time = row_alert['time']
        confidence = calculate_confidence_score(forecast_time, current_time)
        
        # Identifier les types de records à vérifier selon la métrique
        record_types = ['max']
        if alert_column == 'temperature_2m' and 'min' in predicted_records[alert_column]:
            record_types.append('min')
        
        for record_type in record_types:
            # Vérifier que le type de record existe pour cette métrique
            if record_type not in predicted_records[alert_column]:
                continue
                
            value = row_alert[alert_column]
            
            # Créer un ID unique pour ce record basé sur la métrique, type et valeur
            record_id = f"{alert_column}_{record_type}_{value:.2f}_{forecast_time.strftime('%Y%m%d%H')}"
            
            # Vérifier si c'est un nouveau record ou une mise à jour
            existing_record = predicted_records[alert_column][record_type].get(record_id)
            
            if existing_record:
                # Mise à jour d'un record existant
                existing_record['last_seen'] = current_time
                existing_record['confidence'] = max(existing_record['confidence'], confidence)
            else:
                # Nouveau record potentiel
                predicted_records[alert_column][record_type][record_id] = {
                    'value': value,
                    'time': forecast_time,
                    'confidence': confidence,
                    'first_detected': current_time,
                    'last_seen': current_time,
                    'notified': False
                }
                
    except Exception as e:
        await log_message(f"Erreur dans update_predicted_records: {str(e)}")

async def should_notify_record(record_info, metric, record_type):
    """Détermine si un record doit être notifié en évitant le spam."""
    global record_alert_history
    
    try:
        current_time = pd.Timestamp.now(tz='UTC')
        
        # Critères pour notifier :
        # 1. Confiance suffisante (>= 0.6)
        # 2. Pas déjà notifié récemment pour une valeur similaire
        # 3. Record pas trop lointain (< 48h, limite de l'API)
        
        if record_info['confidence'] < 0.6:
            return False
        
        time_diff_hours = abs((record_info['time'] - current_time).total_seconds()) / 3600
        if time_diff_hours > 48:  # Limite de l'API
            return False
        
        # Vérifier l'historique récent (délai adaptatif selon proximité du record)
        # Plus le record est proche, plus on peut notifier fréquemment les changements
        if time_diff_hours <= 3:
            # Record dans les 3h : notifications toutes les 30 minutes
            lookback_hours = 0.5
        elif time_diff_hours <= 12:
            # Record dans les 12h : notifications toutes les 2h
            lookback_hours = 2
        elif time_diff_hours <= 24:
            # Record dans les 24h : notifications toutes les 6h
            lookback_hours = 6
        else:
            # Record plus lointain : notifications toutes les 12h
            lookback_hours = 12
            
        cutoff_time = current_time - pd.Timedelta(hours=lookback_hours)
        recent_alerts = [
            alert for alert in record_alert_history 
            if alert['sent_at'] > cutoff_time 
            and alert['metric'] == metric 
            and alert['type'] == record_type
        ]
        
        # Éviter les notifications répétitives pour des valeurs similaires
        for alert in recent_alerts:
            value_diff = abs(alert['value'] - record_info['value'])
            # Seuils adaptatifs selon la métrique
            if metric == 'temperature_2m':
                threshold = 0.1  # 0.1°C pour température
            elif metric == 'precipitation':
                threshold = 0.5  # 0.5mm pour précipitations
            elif metric == 'windspeed_10m':
                threshold = 2.0  # 2 km/h pour vent
            elif metric == 'pressure_msl':
                threshold = 1.0  # 1 hPa pour pression
            elif metric == 'uv_index':
                threshold = 0.2  # 0.2 pour UV
            else:
                threshold = 0.5  # Défaut
                
            if value_diff < threshold:
                return False
        
        return True
        
    except Exception as e:
        await log_message(f"Erreur dans should_notify_record: {str(e)}")
        return False

async def log_record_alert(metric, record_type, value, forecast_time):
    """Enregistre l'envoi d'une alerte de record."""
    global record_alert_history
    
    try:
        current_time = pd.Timestamp.now(tz='UTC')
        
        # Ajouter à l'historique
        record_alert_history.append({
            'type': record_type,
            'metric': metric,
            'value': value,
            'time': forecast_time,
            'sent_at': current_time
        })
        
        # Nettoyer l'historique (garder seulement les 48 dernières heures)
        cutoff_time = current_time - pd.Timedelta(hours=48)
        record_alert_history = [
            alert for alert in record_alert_history 
            if alert['sent_at'] > cutoff_time
        ]
        
    except Exception as e:
        await log_message(f"Erreur dans log_record_alert: {str(e)}")

async def check_records(row_alert, alert_column, is_forecast=True):
    """Vérifie si une valeur entrante bat un record annuel ET historique absolu.
    Args:
        is_forecast: True pour prévisions (message 'potentiel'), False pour données historiques (message 'nouveau record')
    
    Vérifie 2 types de records :
    - Record annuel : comparaison avec l'année en cours uniquement
    - Record historique absolu : comparaison avec tout l'historique disponible
    
    Version améliorée avec gestion intelligente des notifications pour les prévisions.
    """
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

        # S'assurer que la colonne d'alerte existe et est numérique
        if alert_column not in df.columns:
            await log_message(f"Colonne {alert_column} non trouvée dans le CSV pour vérifier les records.")
            return
        if not pd.api.types.is_numeric_dtype(df[alert_column]):
            await log_message(f"Colonne {alert_column} n'est pas numérique dans le CSV pour vérifier les records.")
            return

        current_year = pd.Timestamp.now(tz='UTC').year
        df_current_year = df[df['time'].dt.year == current_year].copy()
        time_local = row_alert['time'].tz_convert('Europe/Berlin')

        # Mettre à jour le suivi des records prévus si c'est une prévision
        if is_forecast:
            await update_predicted_records(row_alert, alert_column, is_forecast)

        record_detected = False

        # === VÉRIFICATION RECORDS ANNUELS (année courante) ===
        if df_current_year.empty:
            await log_message(f"Aucune donnée pour l'année en cours ({current_year}), impossible de vérifier les records annuels pour {alert_column}.")
            # Si c'est la première donnée de l'année, elle établit le record initial
            if not is_forecast:  # Seulement pour les données réelles
                await send_alert(f"🏆 Info météo : Première donnée de l'année pour {alert_column} : {row_alert[alert_column]} à {time_local.strftime('%H:%M')}.")
        else:
            # Vérification du record max annuel
            max_value_year = df_current_year[alert_column].max()
            if pd.notna(row_alert[alert_column]) and row_alert[alert_column] > max_value_year:
                # Trouver la date de l'ancien record
                max_idx = df_current_year[alert_column].idxmax()
                previous_record_time = df_current_year.loc[max_idx, 'time'].tz_convert('Europe/Berlin')
                previous_record_date = previous_record_time.strftime('%d/%m/%Y')
                
                record_detected = True
                record_info = {
                    'value': row_alert[alert_column],
                    'time': row_alert['time'],
                    'confidence': calculate_confidence_score(row_alert['time'], pd.Timestamp.now(tz='UTC')) if is_forecast else 1.0,
                    'notified': False
                }
                
                if is_forecast:
                    # Pour les prévisions, vérifier si on doit notifier
                    if await should_notify_record(record_info, alert_column, 'max'):
                        prefix = "Potentiel nouveau"
                        suffix = "prévu"
                        confidence_text = f" (confiance: {record_info['confidence']*100:.0f}%)" if record_info['confidence'] < 1.0 else ""
                        await send_alert(f"🏆 Alerte météo : {prefix} record annuel {current_year} (max) pour {alert_column} : {row_alert[alert_column]} (précédent: {max_value_year} le {previous_record_date}) {suffix} à {time_local.strftime('%H:%M')}{confidence_text}.")
                        await log_record_alert(alert_column, 'max', row_alert[alert_column], row_alert['time'])
                        # Marquer comme notifié dans le système de suivi
                        for record_id, stored_record in predicted_records[alert_column]['max'].items():
                            if abs(stored_record['value'] - row_alert[alert_column]) < 0.01:
                                stored_record['notified'] = True
                                break
                else:
                    # Pour les données historiques, toujours notifier
                    prefix = "Nouveau"
                    suffix = "confirmé"
                    await send_alert(f"🏆 Alerte météo : {prefix} record annuel {current_year} (max) pour {alert_column} : {row_alert[alert_column]} (précédent: {max_value_year} le {previous_record_date}) {suffix} à {time_local.strftime('%H:%M')}.")

            # Vérification du record min annuel (spécifiquement pour la température)
            if alert_column == 'temperature_2m':
                min_value_year = df_current_year[alert_column].min()
                if pd.notna(row_alert[alert_column]) and row_alert[alert_column] < min_value_year:
                    # Trouver la date de l'ancien record
                    min_idx = df_current_year[alert_column].idxmin()
                    previous_record_time = df_current_year.loc[min_idx, 'time'].tz_convert('Europe/Berlin')
                    previous_record_date = previous_record_time.strftime('%d/%m/%Y')
                    
                    record_detected = True
                    record_info = {
                        'value': row_alert[alert_column],
                        'time': row_alert['time'],
                        'confidence': calculate_confidence_score(row_alert['time'], pd.Timestamp.now(tz='UTC')) if is_forecast else 1.0,
                        'notified': False
                    }
                    
                    if is_forecast:
                        # Pour les prévisions, vérifier si on doit notifier
                        if await should_notify_record(record_info, alert_column, 'min'):
                            prefix = "Potentiel nouveau"
                            suffix = "prévu"
                            confidence_text = f" (confiance: {record_info['confidence']*100:.0f}%)" if record_info['confidence'] < 1.0 else ""
                            await send_alert(f"🏆 Alerte météo : {prefix} record annuel {current_year} (min) pour {alert_column} : {row_alert[alert_column]} (précédent: {min_value_year} le {previous_record_date}) {suffix} à {time_local.strftime('%H:%M')}{confidence_text}.")
                            await log_record_alert(alert_column, 'min', row_alert[alert_column], row_alert['time'])
                            # Marquer comme notifié dans le système de suivi
                            for record_id, stored_record in predicted_records[alert_column]['min'].items():
                                if abs(stored_record['value'] - row_alert[alert_column]) < 0.01:
                                    stored_record['notified'] = True
                                    break
                    else:
                        # Pour les données historiques, toujours notifier
                        prefix = "Nouveau"
                        suffix = "confirmé"
                        await send_alert(f"🏆 Alerte météo : {prefix} record annuel {current_year} (min) pour {alert_column} : {row_alert[alert_column]} (précédent: {min_value_year} le {previous_record_date}) {suffix} à {time_local.strftime('%H:%M')}.")

        # === VÉRIFICATION RECORDS HISTORIQUES ABSOLUS (tout l'historique) ===
        available_years = sorted(df['time'].dt.year.unique())
        first_year = min(available_years)
        
        # Record max historique absolu
        max_value_absolute = df[alert_column].max()
        if pd.notna(row_alert[alert_column]) and row_alert[alert_column] > max_value_absolute:
            # Trouver la date de l'ancien record historique absolu
            max_absolute_idx = df[alert_column].idxmax()
            previous_absolute_record_time = df.loc[max_absolute_idx, 'time'].tz_convert('Europe/Berlin')
            previous_absolute_record_date = previous_absolute_record_time.strftime('%d/%m/%Y')
            
            record_detected = True
            record_info = {
                'value': row_alert[alert_column],
                'time': row_alert['time'],
                'confidence': calculate_confidence_score(row_alert['time'], pd.Timestamp.now(tz='UTC')) if is_forecast else 1.0,
                'notified': False
            }
            
            if is_forecast:
                # Pour les records historiques absolus, seuil de confiance plus élevé
                if record_info['confidence'] >= 0.7 and await should_notify_record(record_info, alert_column, 'max'):
                    prefix = "Potentiel nouveau"
                    suffix = "prévu"
                    confidence_text = f" (confiance: {record_info['confidence']*100:.0f}%)" if record_info['confidence'] < 1.0 else ""
                    await send_alert(f"🔥 RECORD HISTORIQUE : {prefix} record absolu (max) pour {alert_column} : {row_alert[alert_column]} (précédent: {max_value_absolute} le {previous_absolute_record_date}) {suffix} à {time_local.strftime('%H:%M')} ! Données depuis {first_year}{confidence_text}.")
                    await log_record_alert(alert_column, 'max_absolute', row_alert[alert_column], row_alert['time'])
            else:
                # Pour les données historiques, toujours notifier
                prefix = "Nouveau"
                suffix = "confirmé"
                await send_alert(f"🔥 RECORD HISTORIQUE : {prefix} record absolu (max) pour {alert_column} : {row_alert[alert_column]} (précédent: {max_value_absolute} le {previous_absolute_record_date}) {suffix} à {time_local.strftime('%H:%M')} ! Données depuis {first_year}.")

        # Record min historique absolu (spécifiquement pour la température)
        if alert_column == 'temperature_2m':
            min_value_absolute = df[alert_column].min()
            if pd.notna(row_alert[alert_column]) and row_alert[alert_column] < min_value_absolute:
                # Trouver la date de l'ancien record historique absolu (min)
                min_absolute_idx = df[alert_column].idxmin()
                previous_absolute_min_record_time = df.loc[min_absolute_idx, 'time'].tz_convert('Europe/Berlin')
                previous_absolute_min_record_date = previous_absolute_min_record_time.strftime('%d/%m/%Y')
                
                record_detected = True
                record_info = {
                    'value': row_alert[alert_column],
                    'time': row_alert['time'],
                    'confidence': calculate_confidence_score(row_alert['time'], pd.Timestamp.now(tz='UTC')) if is_forecast else 1.0,
                    'notified': False
                }
                
                if is_forecast:
                    # Pour les records historiques absolus, seuil de confiance plus élevé
                    if record_info['confidence'] >= 0.7 and await should_notify_record(record_info, alert_column, 'min'):
                        prefix = "Potentiel nouveau"
                        suffix = "prévu"
                        confidence_text = f" (confiance: {record_info['confidence']*100:.0f}%)" if record_info['confidence'] < 1.0 else ""
                        await send_alert(f"🥶 RECORD HISTORIQUE : {prefix} record absolu (min) pour {alert_column} : {row_alert[alert_column]} (précédent: {min_value_absolute} le {previous_absolute_min_record_date}) {suffix} à {time_local.strftime('%H:%M')} ! Données depuis {first_year}{confidence_text}.")
                        await log_record_alert(alert_column, 'min_absolute', row_alert[alert_column], row_alert['time'])
                else:
                    # Pour les données historiques, toujours notifier
                    prefix = "Nouveau"
                    suffix = "confirmé"
                    await send_alert(f"🥶 RECORD HISTORIQUE : {prefix} record absolu (min) pour {alert_column} : {row_alert[alert_column]} (précédent: {min_value_absolute} le {previous_absolute_min_record_date}) {suffix} à {time_local.strftime('%H:%M')} ! Données depuis {first_year}.")

    except pd.errors.EmptyDataError:
        await log_message("Fichier CSV vide lors de la vérification des records.")
    except FileNotFoundError:
        await log_message(f"Fichier {csv_filename} non trouvé lors de la vérification des records.")
    except KeyError as e:
        await log_message(f"Erreur de clé dans check_records (colonne manquante?): {str(e)}")
    except Exception as e:
        await log_message(f"Erreur inattendue dans check_records: {str(e)}\n{traceback.format_exc()}")


def calculate_sunshine_hours(df_calc):
    """Calcule les heures d'ensoleillement estimées à partir d'un DataFrame.
    Optimisé pour la localisation configurée avec calculs astronomiques améliorés."""
    # Créer une copie pour éviter SettingWithCopyWarning si df_calc est une slice
    df = df_calc.copy()
    
    # Assurer que 'time' est datetime et localisé
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)

    if df['time'].dt.tz is None:
         df['time'] = df['time'].dt.tz_localize('UTC')

    df['local_time'] = df['time'].dt.tz_convert('Europe/Berlin')
    
    # Coordonnées de la ville pour calculs astronomiques plus précis
    CITY_LAT = float(LATITUDE)  # Latitude depuis config.ini
    
    # Calcul approximatif du lever/coucher du soleil pour la localisation
    # Basé sur l'équation du temps et la déclinaison solaire
    def get_daylight_hours_city(day_of_year):
        """Calcule les heures approximatives de lever/coucher du soleil pour la ville."""
        
        # Déclinaison solaire (approximation)
        declination = 23.45 * math.sin(math.radians(360 * (284 + day_of_year) / 365))
        
        # Angle horaire du coucher du soleil
        try:
            cos_hour_angle = -math.tan(math.radians(CITY_LAT)) * math.tan(math.radians(declination))
            # Limiter entre -1 et 1 pour éviter les erreurs de domaine
            cos_hour_angle = max(-1, min(1, cos_hour_angle))
            hour_angle = math.degrees(math.acos(cos_hour_angle))
            
            # Heures de lever et coucher (approximatives, en heures décimales)
            sunrise_hour = 12 - hour_angle / 15
            sunset_hour = 12 + hour_angle / 15
            
            # Ajustements pour l'équation du temps et l'heure d'été (approximatifs)
            # Correction pour le fuseau horaire Europe/Berlin
            sunrise_hour += 0.5  # Correction approximative
            sunset_hour += 0.5
            
            return max(4, sunrise_hour), min(22, sunset_hour)  # Limites raisonnables
        except:
            # Fallback vers les valeurs saisonnières si le calcul échoue
            if 80 <= day_of_year <= 266:  # Printemps/Été approximatif
                return 6, 20
            else:  # Automne/Hiver
                return 8, 17
    
    # Calcul du jour de l'année et application des heures de jour précises
    df['day_of_year'] = df['local_time'].dt.dayofyear
    df['hour'] = df['local_time'].dt.hour + df['local_time'].dt.minute / 60.0
    df['is_daytime'] = False
    
    # Appliquer le calcul astronomique pour chaque jour unique
    for day_of_year in df['day_of_year'].unique():
        if pd.notna(day_of_year):
            sunrise, sunset = get_daylight_hours_city(int(day_of_year))
            day_mask = df['day_of_year'] == day_of_year
            df.loc[day_mask, 'is_daytime'] = (df.loc[day_mask, 'hour'] >= sunrise) & (df.loc[day_mask, 'hour'] <= sunset)
    
    # Critères d'ensoleillement améliorés pour la région
    if 'uv_index' in df.columns and pd.api.types.is_numeric_dtype(df['uv_index']):
        # Seuils UV adaptatifs selon la saison pour la localisation
        def get_uv_threshold(month):
            """Seuils UV optimisés pour la latitude et climat local."""
            if month in [12, 1, 2]:      # Hiver : soleil plus faible
                return 1.0
            elif month in [3, 4, 10, 11]: # Inter-saisons
                return 1.5
            else:                         # Printemps/Été : seuil plus élevé
                return 2.0
        
        df['month'] = df['local_time'].dt.month
        df['uv_threshold'] = df['month'].apply(get_uv_threshold)
        
        # Conditions d'ensoleillement : jour + UV suffisant + pas de forte pluie
        uv_condition = df['uv_index'].fillna(0) > df['uv_threshold']
        
        # Condition météo : pas de fortes précipitations (nuages épais)
        if 'precipitation' in df.columns and pd.api.types.is_numeric_dtype(df['precipitation']):
            no_heavy_rain = df['precipitation'].fillna(0) < 2.0  # Moins de 2mm/h
        else:
            no_heavy_rain = True
        
        # Condition d'humidité : éviter les brouillards épais (climat lacustre)
        if 'relativehumidity_2m' in df.columns and pd.api.types.is_numeric_dtype(df['relativehumidity_2m']):
            no_thick_fog = df['relativehumidity_2m'].fillna(100) < 95  # Moins de 95% d'humidité
        else:
            no_thick_fog = True
        
        # Combinaison des conditions pour un ensoleillement effectif
        df['is_sunny'] = df['is_daytime'] & uv_condition & no_heavy_rain & no_thick_fog
        sunshine_hours = df['is_sunny'].sum()
    else:
        # Fallback si pas de données UV : estimation basée sur les conditions météo seules
        if ('precipitation' in df.columns and
            'relativehumidity_2m' in df.columns and
            pd.api.types.is_numeric_dtype(df['precipitation']) and
            pd.api.types.is_numeric_dtype(df['relativehumidity_2m'])):
            
            # Estimation sans UV : conditions météo favorables
            good_weather = (df['precipitation'].fillna(0) < 1.0) & (df['relativehumidity_2m'].fillna(100) < 85)
            df['is_sunny'] = df['is_daytime'] & good_weather
            sunshine_hours = df['is_sunny'].sum() * 0.7  # Facteur de correction sans UV
        else:
            # Estimation très approximative basée sur les heures de jour seules
            sunshine_hours = df['is_daytime'].sum() * 0.4  # 40% d'ensoleillement moyen

    return sunshine_hours


def is_month_complete(df_month, year, month, metric_column=None):
    """Vérifie si un mois a au moins une donnée par jour pour une métrique spécifique (sauf pour le mois en cours)."""
    if df_month.empty:
        return False
    
    # Le mois en cours est toujours accepté (même s'il est incomplet)
    current_time = pd.Timestamp.now(tz='UTC')
    current_year = current_time.year
    current_month = current_time.month
    
    if year == current_year and month == current_month:
        return True
    
    # Pour les mois historiques, vérifier qu'on a au moins une donnée par jour pour la métrique
    days_in_month = calendar.monthrange(year, month)[1]
    
    # Si une métrique spécifique est demandée, filtrer seulement sur cette métrique
    if metric_column and metric_column in df_month.columns:
        df_filtered = df_month.dropna(subset=[metric_column]).copy()
    else:
        df_filtered = df_month.copy()
    
    if df_filtered.empty:
        return False
    
    # Compter les jours uniques avec des données (pour cette métrique)
    df_filtered['date'] = df_filtered['time'].dt.date
    unique_days = df_filtered['date'].nunique()
    
    # Exiger au moins 80% des jours du mois pour considérer le mois comme complet
    # (permet une petite tolérance pour les pannes/maintenance)
    required_days = max(1, int(days_in_month * 0.8))
    
    return unique_days >= required_days

def filter_complete_months_only(df_input, exclude_current_month=False, metric_column=None):
    """Filtre un DataFrame pour ne garder que les mois avec des données complètes pour une métrique spécifique.
    
    Args:
        df_input: DataFrame avec colonne 'time'
        exclude_current_month: Si True, exclut aussi le mois en cours
        metric_column: Si spécifié, filtre seulement sur cette métrique
    
    Returns:
        DataFrame filtré
    """
    if df_input.empty:
        return df_input
    
    df = df_input.copy()
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)
    
    if df['time'].dt.tz is None:
        df['time'] = df['time'].dt.tz_localize('UTC')
    
    # Ajouter colonnes année/mois pour le groupement
    df['year'] = df['time'].dt.year
    df['month'] = df['time'].dt.month
    
    # Identifier les mois complets pour la métrique spécifiée
    complete_months = []
    
    for (year, month), group in df.groupby(['year', 'month']):
        if is_month_complete(group, year, month, metric_column):
            # Si on veut exclure le mois en cours et que c'est le mois en cours, skip
            current_time = pd.Timestamp.now(tz='UTC')
            if exclude_current_month and year == current_time.year and month == current_time.month:
                continue
            complete_months.append((year, month))
    
    # Filtrer le DataFrame pour ne garder que les mois complets
    if not complete_months:
        return pd.DataFrame(columns=df.columns)
    
    mask = df[['year', 'month']].apply(tuple, axis=1).isin(complete_months)
    return df[mask].drop(['year', 'month'], axis=1)

def calculate_monthly_sunshine(df_calc):
    """Calcule les heures d'ensoleillement par mois (seulement pour les mois complets)."""
    df = df_calc.copy()
    if df.empty:
        return pd.Series(dtype=float)

    # Filtrer pour ne garder que les mois complets (incluant le mois en cours) pour l'ensoleillement (UV principalement)
    df_complete = filter_complete_months_only(df, exclude_current_month=False, metric_column='uv_index')
    
    if df_complete.empty:
        return pd.Series(dtype=float)

    if not pd.api.types.is_datetime64_any_dtype(df_complete['time']):
        df_complete['time'] = pd.to_datetime(df_complete['time'], utc=True, errors='coerce')
    df_complete.dropna(subset=['time'], inplace=True)
    if df_complete['time'].dt.tz is None:
         df_complete['time'] = df_complete['time'].dt.tz_localize('UTC')
    
    # Grouper par mois et appliquer le calcul d'ensoleillement
    # apply peut être lent sur de gros dataframes, mais pour des données mensuelles, c'est acceptable.
    monthly_sunshine = df_complete.groupby(pd.Grouper(key='time', freq='MS')).apply(calculate_sunshine_hours) # MS for Month Start
    
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

    # Calculs mensuels pour ensoleillement et précipitations
    df['local_time'] = df['time'].dt.tz_convert('Europe/Berlin')
    df['year_month'] = df['local_time'].dt.to_period('M')
    
    # Calculer l'ensoleillement mensuel
    sunniest_month = "N/A"
    rainiest_month = "N/A"
    
    try:
        monthly_sunshine = df.groupby('year_month').apply(calculate_sunshine_hours)
        if not monthly_sunshine.empty:
            sunniest_period = monthly_sunshine.idxmax()
            sunniest_month = f"{sunniest_period.strftime('%B %Y')} ({monthly_sunshine.max():.1f}h)"
    except Exception:
        pass
    
    # Calculer les précipitations mensuelles
    try:
        if 'precipitation' in df.columns and pd.api.types.is_numeric_dtype(df['precipitation']):
            monthly_precip = df.groupby('year_month')['precipitation'].sum()
            if not monthly_precip.empty:
                rainiest_period = monthly_precip.idxmax()
                rainiest_month = f"{rainiest_period.strftime('%B %Y')} ({monthly_precip.max():.1f}mm)"
    except Exception:
        pass

    max_temp, idx_hot_day = get_stat(df, 'temperature_2m', 'max'), get_stat(df, 'temperature_2m', 'idxmax')
    min_temp, idx_cold_day = get_stat(df, 'temperature_2m', 'min'), get_stat(df, 'temperature_2m', 'idxmin')
    max_precipitation, idx_rain_day = get_stat(df, 'daily_precipitation', 'max'), get_stat(df, 'daily_precipitation', 'idxmax')
    max_uv_index, idx_uv_day = get_stat(df, 'uv_index', 'max'), get_stat(df, 'uv_index', 'idxmax')
    max_wind_speed, idx_wind_day = get_stat(df, 'windspeed_10m', 'max'), get_stat(df, 'windspeed_10m', 'idxmax')
    avg_temp = get_stat(df, 'temperature_2m', 'mean')
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
        f"🌧️ Mois le plus pluvieux: {rainiest_month}",
        f"💦 Jour le plus humide: {humid_day_berlin} ({max_humidity:.1f}%)" if pd.notna(max_humidity) else "💦 Humidité max: N/A",
        f"🏜️ Jour le plus sec: {dry_day_berlin} ({min_humidity:.1f}%)" if pd.notna(min_humidity) else "🏜️ Humidité min: N/A",
        f"☀️ Mois le plus ensoleillé: {sunniest_month}"
    ]
    return "\n".join(summary_parts)

# --- Fonctions utilitaires pour graphiques et dates ---
def parse_date_input(date_str):
    """Parse et valide une date d'entrée (format YYYY-MM-DD)."""
    try:
        return pd.to_datetime(date_str, format='%Y-%m-%d', utc=True)
    except (ValueError, TypeError):
        return None

async def create_graph_image(fig):
    """Convertit une figure matplotlib en BytesIO pour Telegram."""
    buf = io.BytesIO()
    
    # Remplacer les emojis dans tous les textes du graphique avant de sauvegarder
    try:
        for ax in fig.get_axes():
            # Titre principal
            if ax.get_title():
                ax.set_title(replace_emojis_for_matplotlib(ax.get_title()))
            
            # Labels des axes
            if ax.get_xlabel():
                ax.set_xlabel(replace_emojis_for_matplotlib(ax.get_xlabel()))
            if ax.get_ylabel():
                ax.set_ylabel(replace_emojis_for_matplotlib(ax.get_ylabel()))
            
            # Légende
            legend = ax.get_legend()
            if legend:
                for text in legend.get_texts():
                    text.set_text(replace_emojis_for_matplotlib(text.get_text()))
            
            # Annotations et textes
            for text in ax.texts:
                text.set_text(replace_emojis_for_matplotlib(text.get_text()))
            
            # Labels des ticks si ils contiennent des emojis
            for label in ax.get_xticklabels():
                label.set_text(replace_emojis_for_matplotlib(label.get_text()))
            for label in ax.get_yticklabels():
                label.set_text(replace_emojis_for_matplotlib(label.get_text()))
        
        # Titre général de la figure
        if hasattr(fig, '_suptitle') and fig._suptitle:
            fig.suptitle(replace_emojis_for_matplotlib(fig._suptitle.get_text()))
    except Exception as e:
        await log_message(f"Erreur lors du remplacement des emojis dans le graphique: {str(e)}")
    
    # Sauvegarder avec gestion d'erreur améliorée
    try:
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    except Exception as e:
        await log_message(f"Erreur lors de la sauvegarde du graphique: {str(e)}")
        # Fallback: essayer une sauvegarde simple sans bbox_inches
        try:
            fig.savefig(buf, format='png', dpi=100)
        except Exception as e2:
            await log_message(f"Erreur fallback sauvegarde graphique: {str(e2)}")
            raise e2
    
    buf.seek(0)
    plt.close(fig)
    return buf

async def send_graph(chat_id, fig, caption):
    """Envoie un graphique via Telegram."""
    try:
        buf = await create_graph_image(fig)
        input_file = BufferedInputFile(buf.getvalue(), filename="graph.png")
        await bot.send_photo(chat_id=chat_id, photo=input_file, caption=caption)
        await log_message(f"Graphique envoyé avec succès à chat_id: {chat_id}")
    except Exception as e:
        await log_message(f"Erreur envoi graphique à {chat_id}: {str(e)}")
        await bot.send_message(chat_id, f"Erreur lors de la génération du graphique: {str(e)}")

def get_metric_info(metric):
    """Retourne les informations d'affichage pour une métrique."""
    metrics = {
        'temperature_2m': {'name': 'Température', 'unit': '°C', 'emoji': '🌡️'},
        'precipitation': {'name': 'Précipitations', 'unit': 'mm', 'emoji': '🌧️'},
        'windspeed_10m': {'name': 'Vitesse du vent', 'unit': 'km/h', 'emoji': '💨'},
        'pressure_msl': {'name': 'Pression', 'unit': 'hPa', 'emoji': '🎈'},
        'uv_index': {'name': 'Indice UV', 'unit': '', 'emoji': '☀️'},
        'relativehumidity_2m': {'name': 'Humidité', 'unit': '%', 'emoji': '💦'},
        'precipitation_probability': {'name': 'Probabilité pluie', 'unit': '%', 'emoji': '🌦️'}
    }
    return metrics.get(metric, {'name': metric, 'unit': '', 'emoji': '📊'})

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
            "📊 **Commandes de base :**\n"
            "/weather - Météo actuelle (prévisions du cache)\n"
            "/forecast - Prévisions détaillées des prochaines heures\n"
            "/sunshine - Graphique ensoleillement mensuel par année\n\n"
            "📅 **Résumés par période :**\n"
            "/month - Résumé du mois dernier\n"
            "/year - Résumé de l'année en cours\n"
            "/all - Résumé de toutes les données historiques\n"
            "/daterange YYYY-MM-DD YYYY-MM-DD - Résumé période personnalisée\n\n"
            "📈 **Graphiques et analyses :**\n"
            "/forecastgraph - Graphique prévisions 24h (température + précipitations)\n"
            "/graph <métrique> [jours] - 2 graphiques: courbe + barres\n"
            "   Métriques: temperature, rain, wind, pressure, uv, humidity\n"
            "/heatmap [année|all] - Calendrier thermique (style GitHub)\n"
            "/yearcompare [métrique] - Comparaison entre années\n"
            "/sunshinelist - Liste mensuelle d'ensoleillement estimé\n"
            "/top10 <métrique> - Classement des valeurs extrêmes\n\n"
            f"💡 **Exemples pratiques :**\n"
            f"/weather - Météo actuelle avec heure de mise à jour\n"
            f"/graph rain 7 - Pluie: courbe + barres des 7 derniers jours\n"
            f"/heatmap all - Calendrier multi-années des températures\n"
            f"/yearcompare temperature - Comparer les années\n"
            f"/daterange 2024-06-01 2024-08-31 - Analyse été 2024\n\n"
            f"🎯 **Info importante :** Les graphiques affichent `[Temp]`, `[Rain]` etc. au lieu d'emojis pour éviter les erreurs d'affichage.\n\n"
            f"Explorez la météo de {welcome_ville} avec ces outils d'analyse avancés!"
        )
        await message.reply(welcome_message)
    else:
        welcome_back_message = (
            "Bon retour ! 🌤️ Votre bot météo est prêt.\n\n"
            "⚡ **Commandes rapides :**\n"
            "• /weather - Météo actuelle (avec heure de mise à jour)\n"
            "• /forecast - Prévisions détaillées\n"
            "• /graph temp 7 - Courbe + barres température 7j\n"
            "• /heatmap all - Calendrier thermique multi-années\n"
            "• /yearcompare rain - Comparer les précipitations\n"
            "• /forecastgraph - Graphique prévisions 24h\n\n"
            "🎯 **Rappel :** Les graphiques utilisent `[Temp]`, `[Rain]` etc. pour éviter les erreurs d'affichage.\n\n"
            "💡 **Métriques :** temperature, rain, wind, pressure, uv, humidity\n\n"
            "Que souhaitez-vous analyser aujourd'hui ?"
        )
        await message.reply(welcome_back_message)

@router.message(Command("weather"))
async def get_latest_info_command(message: types.Message):
    try:
        await log_message("Début de get_latest_info_command avec double contexte temporel")
        
        # Utiliser le cache des prévisions pour obtenir les données actuelles
        df_seven, df_twenty_four = await get_cached_forecast_data()
        
        # Récupérer la dernière heure enregistrée depuis les données historiques
        last_recorded_time = await get_last_recorded_time()
        
        # Obtenir l'heure de la dernière mise à jour du cache
        cache_update_time = cached_forecast_data['last_update']
        if cache_update_time:
            cache_update_display = cache_update_time.tz_convert('Europe/Berlin').strftime("%H:%M:%S")
        else:
            cache_update_display = "Inconnue"
        
        response_parts = [f"🌡️ **Météo actuelle à {VILLE}**\n"]
        
        # === SECTION 1: PRÉVISIONS ===
        forecast_info = None
        if not df_seven.empty:
            # Données de prévision 7h
            forecast_info = df_seven.iloc[0].to_dict()
            forecast_time_display = forecast_info['time'].tz_convert('Europe/Berlin').strftime("%d/%m à %H:%M")
            data_source = "prévisions 7h (cache)"
        elif not df_twenty_four.empty:
            # Fallback vers cache 24h
            forecast_info = df_twenty_four.iloc[0].to_dict()
            forecast_time_display = forecast_info['time'].tz_convert('Europe/Berlin').strftime("%d/%m à %H:%M")
            data_source = "prévisions 24h (cache)"
        
        if forecast_info:
            response_parts.append(f"📊 **PRÉVISIONS:**")
            response_parts.append(f"📅 Prévision pour: {forecast_time_display}")
            response_parts.append(f"🔄 Dernière maj: {cache_update_display}")
            response_parts.append(f"🌡️ Température: {forecast_info.get('temperature_2m', 'N/A')}°C")
            response_parts.append(f"🌧️ Probabilité de pluie: {forecast_info.get('precipitation_probability', 'N/A')}%")
            response_parts.append(f"💧 Précipitations: {forecast_info.get('precipitation', 'N/A')}mm")
            response_parts.append(f"💨 Vent: {forecast_info.get('windspeed_10m', 'N/A')}km/h")
            response_parts.append(f"☀️ Indice UV: {forecast_info.get('uv_index', 'N/A')}")
            response_parts.append(f"🎈 Pression: {forecast_info.get('pressure_msl', 'N/A')} hPa")
            response_parts.append(f"💦 Humidité: {forecast_info.get('relativehumidity_2m', 'N/A')}%")
            response_parts.append("")  # Ligne vide de séparation
        
        # === SECTION 2: DERNIÈRE MESURE ENREGISTRÉE ===
        response_parts.append(f"📊 **DERNIÈRE MESURE ENREGISTRÉE:**")
        
        if last_recorded_time:
            # Lire les dernières données historiques du CSV
            try:
                if os.path.exists(csv_filename) and os.path.getsize(csv_filename) > 0:
                    df = pd.read_csv(csv_filename)
                    if not df.empty:
                        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
                        df.dropna(subset=['time'], inplace=True)
                        df.sort_values(by='time', inplace=True)
                        
                        if not df.empty:
                            historical_info = df.iloc[-1].to_dict()
                            response_parts.append(f"📅 Dernière mesure: {last_recorded_time}")
                            response_parts.append(f"🌡️ Température: {historical_info.get('temperature_2m', 'N/A')}°C")
                            response_parts.append(f"🌧️ Probabilité de pluie: {historical_info.get('precipitation_probability', 'N/A')}%")
                            response_parts.append(f"💧 Précipitations: {historical_info.get('precipitation', 'N/A')}mm")
                            response_parts.append(f"💨 Vent: {historical_info.get('windspeed_10m', 'N/A')}km/h")
                            response_parts.append(f"☀️ Indice UV: {historical_info.get('uv_index', 'N/A')}")
                            response_parts.append(f"🎈 Pression: {historical_info.get('pressure_msl', 'N/A')} hPa")
                            response_parts.append(f"💦 Humidité: {historical_info.get('relativehumidity_2m', 'N/A')}%")
                        else:
                            response_parts.append(f"❌ Données historiques invalides")
                    else:
                        response_parts.append(f"❌ Fichier de données historiques vide")
                else:
                    response_parts.append(f"❌ Aucun fichier de données historiques")
            except Exception as e:
                await log_message(f"Erreur lecture données historiques: {str(e)}")
                response_parts.append(f"❌ Erreur lecture données historiques")
        else:
            response_parts.append(f"❌ Aucune mesure historique disponible")
        
        # Si aucune prévision n'est disponible, afficher un message d'erreur
        if not forecast_info:
            response_parts.insert(1, f"❌ **Aucune prévision disponible actuellement**\n")
            await log_message("Aucune prévision disponible dans les caches pour /weather")
        
        await log_message(f"Réponse /weather avec double contexte temporel préparée, tentative d'envoi")
        await message.reply("\n".join(response_parts))
        await log_message("Réponse /weather avec double contexte temporel envoyée avec succès")

    except Exception as e:
        await log_message(f"Error in get_latest_info_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply(f"Erreur lors de l'obtention des informations : {str(e)}")


@router.message(Command("forecast"))
async def get_forecast_command(message: types.Message): # Renommé pour clarté
    try:
        # Utiliser le cache au lieu d'appeler directement l'API
        df_next_seven_hours, _ = await get_cached_forecast_data()
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
async def get_sunshine_summary_command(message: types.Message):
    """Génère un graphique en barres de l'ensoleillement mensuel par année."""
    try:
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await message.reply("Aucune donnée disponible pour calculer l'ensoleillement.")
            return
            
        df = pd.read_csv(csv_filename)
        if df.empty:
            await message.reply("Aucune donnée disponible pour calculer l'ensoleillement (fichier vide).")
            return

        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)

        if df.empty:
            await message.reply("Aucune donnée temporelle valide pour calculer l'ensoleillement.")
            return

        # Filtrer pour ne garder que les mois complets (incluant le mois en cours) pour l'ensoleillement (UV principalement)
        df_complete = filter_complete_months_only(df, exclude_current_month=False, metric_column='uv_index')

        # Convertir en heure locale et extraire année/mois
        df_complete['local_time'] = df_complete['time'].dt.tz_convert('Europe/Berlin')
        df_complete['year'] = df_complete['local_time'].dt.year
        df_complete['month'] = df_complete['local_time'].dt.month
        
        # Vérifier qu'on a au moins quelques mois de données
        available_years = sorted(df_complete['year'].unique())
        if len(available_years) == 0:
            await message.reply(f"Pas encore de données d'ensoleillement calculées pour {VILLE}.")
            return

        # Calculer l'ensoleillement mensuel pour chaque année
        monthly_sunshine_data = []
        
        for year in available_years:
            year_data = df_complete[df_complete['year'] == year].copy()
            if not year_data.empty:
                # Grouper par mois et calculer l'ensoleillement pour chaque mois
                for month in range(1, 13):
                    month_data = year_data[year_data['month'] == month]
                    if not month_data.empty:  # Suppression du check >= 24 car le filtrage est fait en amont
                        sunshine_hours = calculate_sunshine_hours(month_data)
                        monthly_sunshine_data.append({
                            'year': year,
                            'month': month,
                            'sunshine_hours': sunshine_hours
                        })

        if not monthly_sunshine_data:
            await message.reply("Pas assez de données pour calculer l'ensoleillement mensuel.")
            return

        sunshine_df = pd.DataFrame(monthly_sunshine_data)
        
        # Préparer les données pour le graphique en barres groupées
        pivot_data = sunshine_df.pivot(index='month', columns='year', values='sunshine_hours')
        
        # Créer le graphique moderne en barres groupées
        fig, ax = plt.subplots(1, 1, figsize=(16, 10))
        fig.patch.set_facecolor('#f8f9fa')
        
        # Palette de couleurs moderne pour l'ensoleillement
        sunshine_colors = ['#f39c12', '#e67e22', '#d35400', '#f1c40f', '#f4d03f',
                          '#3498db', '#2980b9', '#9b59b6', '#8e44ad', '#2ecc71']
        
        # Noms des mois
        month_names = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
                      'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']
        
        # Créer les barres groupées
        x = np.arange(len(month_names))  # positions des mois
        n_years = len(available_years)
        width = 0.8 / n_years  # largeur de chaque barre
        
        current_year = pd.Timestamp.now(tz='Europe/Berlin').year
        
        # Tracer les barres pour chaque année
        for i, year in enumerate(available_years):
            if year in pivot_data.columns:
                values = []
                for month in range(1, 13):
                    if month in pivot_data.index and pd.notna(pivot_data.loc[month, year]):
                        values.append(pivot_data.loc[month, year])
                    else:
                        values.append(0)  # 0 pour les mois sans données
                
                color = sunshine_colors[i % len(sunshine_colors)]
                offset = (i - n_years/2 + 0.5) * width
                
                # Style spécial pour l'année courante
                if year == current_year:
                    bars = ax.bar(x + offset, values, width,
                                 label=f'☀️ {year} (actuelle)', color=color,
                                 alpha=0.9, edgecolor='white', linewidth=2,
                                 zorder=5)
                    # Ajouter des valeurs sur les barres de l'année courante
                    for j, bar in enumerate(bars):
                        if values[j] > 0:
                            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                                   f'{values[j]:.0f}h', ha='center', va='bottom',
                                   fontsize=9, fontweight='bold', color=color)
                else:
                    ax.bar(x + offset, values, width,
                          label=f'🌤️ {year}', color=color,
                          alpha=0.7, edgecolor='white', linewidth=1,
                          zorder=3)
        
        # Mise en évidence du mois actuel
        current_month = pd.Timestamp.now(tz='Europe/Berlin').month
        ax.axvspan(current_month - 1 - 0.4, current_month - 1 + 0.4,
                  alpha=0.2, color='#f39c12', zorder=1,
                  label=f"📍 Mois actuel ({month_names[current_month-1]})")
        
        # Titre moderne avec emojis
        ax.set_title(f'☀️ Ensoleillement mensuel par année - {VILLE}\n'
                    f'📊 Comparaison de {len(available_years)} années de données',
                    fontsize=18, fontweight='bold', color='#2c3e50', pad=25)
        
        # Labels modernes avec emojis
        ax.set_xlabel('📅 Mois', fontsize=14, color='#2c3e50', fontweight='bold')
        ax.set_ylabel('☀️ Heures d\'ensoleillement', fontsize=14, color='#2c3e50', fontweight='bold')
        
        # Configurer l'axe X avec les noms des mois
        ax.set_xticks(x)
        ax.set_xticklabels(month_names)
        
        # Style moderne des axes
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#bdc3c7')
        ax.spines['bottom'].set_color('#bdc3c7')
        ax.tick_params(colors='#34495e', labelsize=11)
        
        # Légende moderne
        legend = ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left',
                          frameon=True, shadow=True, fancybox=True,
                          framealpha=0.95, edgecolor='#bdc3c7')
        legend.get_frame().set_facecolor('#ffffff')
        
        # Zones saisonnières en arrière-plan
        ax.axvspan(-0.5, 1.5, alpha=0.05, color='#74b9ff')    # Hiver
        ax.axvspan(1.5, 4.5, alpha=0.05, color='#55a3ff')     # Printemps
        ax.axvspan(4.5, 7.5, alpha=0.05, color='#fdcb6e')     # Été
        ax.axvspan(7.5, 10.5, alpha=0.05, color='#e17055')    # Automne
        ax.axvspan(10.5, 11.5, alpha=0.05, color='#74b9ff')   # Hiver
        
        # Grille moderne
        ax.grid(True, alpha=0.3, linestyle=':', linewidth=1, axis='y')
        
        plt.tight_layout()
        
        # Statistiques pour la légende
        stats_text = ""
        if len(available_years) > 1:
            # Calculer la moyenne par mois sur toutes les années
            monthly_averages = sunshine_df.groupby('month')['sunshine_hours'].mean()
            best_month = monthly_averages.idxmax()
            worst_month = monthly_averages.idxmin()
            
            stats_text += f"📊 Statistiques globales:\n"
            stats_text += f"☀️ Meilleur mois: {month_names[best_month-1]} ({monthly_averages[best_month]:.1f}h)\n"
            stats_text += f"☁️ Moins ensoleillé: {month_names[worst_month-1]} ({monthly_averages[worst_month]:.1f}h)\n"
            
            # Données pour le mois actuel
            current_month_data = sunshine_df[sunshine_df['month'] == current_month]
            if not current_month_data.empty:
                current_month_avg = current_month_data['sunshine_hours'].mean()
                stats_text += f"📍 Mois actuel (moy.): {current_month_avg:.1f}h"
        
        caption = (f"☀️ Ensoleillement mensuel en barres - {len(available_years)} années\n"
                  f"📅 Années: {min(available_years)}-{max(available_years)}\n"
                  f"{stats_text}")
        
        await send_graph(message.chat.id, fig, caption)
        
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

@router.message(Command("daterange"))
async def get_daterange_summary_command(message: types.Message):
    """Génère un résumé entre deux dates. Usage: /daterange 2024-01-01 2024-12-31"""
    try:
        # Parser les arguments
        args = message.text.split()
        if len(args) != 3:
            await message.reply(
                "Usage: /daterange YYYY-MM-DD YYYY-MM-DD\n"
                "Exemple: /daterange 2024-01-01 2024-12-31"
            )
            return
        
        start_date = parse_date_input(args[1])
        end_date = parse_date_input(args[2])
        
        if not start_date or not end_date:
            await message.reply(
                "Format de date invalide. Utilisez YYYY-MM-DD\n"
                "Exemple: /daterange 2024-01-01 2024-12-31"
            )
            return
        
        if start_date > end_date:
            await message.reply("La date de début doit être antérieure à la date de fin.")
            return
        
        # Lire et filtrer les données
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await message.reply("Aucune donnée météo disponible.")
            return
        
        df = pd.read_csv(csv_filename)
        if df.empty:
            await message.reply("Aucune donnée disponible dans le fichier.")
            return
        
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)
        
        # Filtrer selon la période
        df_period = df[(df['time'] >= start_date) & (df['time'] <= end_date)].copy()
        
        if df_period.empty:
            period_str = f"{start_date.strftime('%Y-%m-%d')} à {end_date.strftime('%Y-%m-%d')}"
            await message.reply(f"Aucune donnée disponible pour la période sélectionnée ({period_str}).")
            return
        
        # Générer le résumé
        summary = generate_summary(df_period)
        period_str = f"du {start_date.strftime('%d/%m/%Y')} au {end_date.strftime('%d/%m/%Y')}"
        
        message_text = f"📅 Résumé météo {period_str} pour {VILLE}:\n\n{summary}"
        await message.reply(message_text)
        await log_message(f"Résumé période {period_str} envoyé à chat_id: {message.chat.id}")
        
    except Exception as e:
        await log_message(f"Erreur dans daterange_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply("Erreur lors de la génération du résumé pour cette période.")

@router.message(Command("top10"))
async def get_top10_command(message: types.Message):
    """Affiche le top 10 des valeurs pour une métrique. Usage: /top10 temperature"""
    try:
        # Parser les arguments
        args = message.text.split()
        if len(args) != 2:
            metrics_list = "temperature, rain, wind, pressure, uv, humidity"
            await message.reply(
                f"Usage: /top10 <métrique>\n"
                f"Métriques disponibles: {metrics_list}\n"
                f"Exemple: /top10 temperature"
            )
            return
        
        metric_arg = args[1].lower()
        
        # Mapping des arguments vers les colonnes CSV
        metric_mapping = {
            'temperature': 'temperature_2m',
            'temp': 'temperature_2m',
            'rain': 'precipitation',
            'precipitation': 'precipitation',
            'wind': 'windspeed_10m',
            'pressure': 'pressure_msl',
            'uv': 'uv_index',
            'humidity': 'relativehumidity_2m'
        }
        
        if metric_arg not in metric_mapping:
            await message.reply(
                f"Métrique '{metric_arg}' non reconnue.\n"
                f"Utilisez: temperature, rain, wind, pressure, uv, humidity"
            )
            return
        
        column_name = metric_mapping[metric_arg]
        metric_info = get_metric_info(column_name)
        
        # Lire les données
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await message.reply("Aucune donnée météo disponible.")
            return
        
        df = pd.read_csv(csv_filename)
        if df.empty:
            await message.reply("Aucune donnée disponible.")
            return
        
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)
        
        # Filtrer pour ne garder que les mois complets (incluant le mois en cours) pour cette métrique
        df_complete = filter_complete_months_only(df, exclude_current_month=False, metric_column=column_name)
        
        if column_name not in df_complete.columns:
            await message.reply(f"Données pour {metric_info['name']} non disponibles.")
            return
        
        # Nettoyer les données et enlever les NaN
        df_clean = df_complete.dropna(subset=[column_name]).copy()
        if df_clean.empty:
            await message.reply(f"Aucune donnée valide pour {metric_info['name']}.")
            return
        
        # Top 10 des valeurs maximales
        top_max = df_clean.nlargest(10, column_name)
        # Top 10 des valeurs minimales (pour température principalement)
        top_min = df_clean.nsmallest(10, column_name) if metric_arg in ['temperature', 'temp'] else None
        
        # Construire le message
        response = f"{metric_info['emoji']} **Top 10 - {metric_info['name']}** à {VILLE}\n\n"
        
        # Top maximales
        response += f"🔥 **Valeurs les plus élevées:**\n"
        for i, (_, row) in enumerate(top_max.iterrows(), 1):
            time_local = row['time'].tz_convert('Europe/Berlin')
            date_str = time_local.strftime('%d/%m/%Y à %H:%M')
            value = row[column_name]
            response += f"{i:2d}. {value:.1f}{metric_info['unit']} - {date_str}\n"
        
        # Top minimales (seulement pour température)
        if top_min is not None:
            response += f"\n❄️ **Valeurs les plus basses:**\n"
            for i, (_, row) in enumerate(top_min.iterrows(), 1):
                time_local = row['time'].tz_convert('Europe/Berlin')
                date_str = time_local.strftime('%d/%m/%Y à %H:%M')
                value = row[column_name]
                response += f"{i:2d}. {value:.1f}{metric_info['unit']} - {date_str}\n"
        
        await message.reply(response)
        await log_message(f"Top 10 {metric_info['name']} envoyé à chat_id: {message.chat.id}")
        
    except Exception as e:
        await log_message(f"Erreur dans top10_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply("Erreur lors de la génération du top 10.")

@router.message(Command("forecastgraph"))
async def get_forecast_graph_command(message: types.Message):
    """Génère un graphique des prévisions météo pour les prochaines 24h."""
    try:
        # Utiliser le cache au lieu d'appeler directement l'API
        _, df_next_twenty_four_hours = await get_cached_forecast_data()
        
        if df_next_twenty_four_hours.empty:
            await message.reply("Aucune donnée de prévision disponible pour le moment.")
            return
        
        # Limiter aux 24 prochaines heures
        forecast_df = df_next_twenty_four_hours.head(24).copy()
        
        if forecast_df.empty:
            await message.reply("Données de prévision insuffisantes.")
            return
        
        # Convertir en heure locale pour l'affichage
        forecast_df['local_time'] = forecast_df['time'].dt.tz_convert('Europe/Berlin')
        
        # Créer le graphique avec style moderne
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        fig.suptitle(f'🌤️ Prévisions météo pour {VILLE} - Prochaines 24h',
                    fontsize=18, fontweight='bold', y=0.95, color='#2c3e50')
        
        # Graphique température avec gradient
        temps = forecast_df['local_time']
        temperatures = forecast_df['temperature_2m']
        
        # Ligne principale température avec gradient de couleur
        points = ax1.scatter(temps, temperatures, c=temperatures, cmap='RdYlBu_r',
                           s=60, alpha=0.8, edgecolors='white', linewidth=1.5, zorder=5)
        ax1.plot(temps, temperatures, linewidth=3, alpha=0.7, color='#34495e', zorder=4)
        
        # Zone de fond gradient selon température
        for i in range(len(forecast_df)-1):
            temp = forecast_df.iloc[i]['temperature_2m']
            if temp > 25:
                color = '#ff6b6b'  # Rouge moderne
                alpha = 0.15
            elif temp < 5:
                color = '#74b9ff'  # Bleu moderne
                alpha = 0.15
            else:
                color = '#55a3ff'  # Vert moderne
                alpha = 0.08
            ax1.axvspan(temps.iloc[i], temps.iloc[i+1], color=color, alpha=alpha)
        
        ax1.set_ylabel('🌡️ Température (°C)', fontsize=12, color='#2c3e50')
        ax1.tick_params(colors='#34495e')
        
        # Colorbar pour la température
        cbar = plt.colorbar(points, ax=ax1, pad=0.02, aspect=30)
        cbar.set_label('°C', rotation=0, labelpad=15, color='#2c3e50')
        
        # Graphique précipitations moderne avec gradient MeteoSuisse
        def get_precipitation_color(precip_mm):
            """Couleurs graduées selon l'intensité des précipitations (style MeteoSuisse)."""
            if precip_mm < 0.1:
                return '#f8f9fa'      # Blanc/transparent (pas de pluie)
            elif precip_mm < 1.0:
                return '#cce7ff'      # Bleu très clair (bruine)
            elif precip_mm < 2.5:
                return '#74b9ff'      # Bleu clair (faible)
            elif precip_mm < 5.0:
                return '#4a90e2'      # Bleu moyen (modéré)
            elif precip_mm < 10.0:
                return '#2563eb'      # Bleu foncé (fort)
            elif precip_mm < 25.0:
                return '#7c3aed'      # Violet (très fort)
            elif precip_mm < 50.0:
                return '#dc2626'      # Rouge (intense)
            else:
                return '#991b1b'      # Rouge foncé (extrême)
        
        colors_precip = [get_precipitation_color(p) for p in forecast_df['precipitation']]
        bars = ax2.bar(temps, forecast_df['precipitation'], width=0.03,
                      color=colors_precip, alpha=0.9, edgecolor='white', linewidth=0.5)
        
        # Ajouter légende des couleurs précipitations (style MeteoSuisse)
        max_precip = forecast_df['precipitation'].max()
        if max_precip > 0.1:  # Seulement si il y a des précipitations
            legend_text = "Précipitations: "
            if max_precip >= 25.0:
                legend_text += "🔴 Extrême"
            elif max_precip >= 10.0:
                legend_text += "🟣 Très fort"
            elif max_precip >= 5.0:
                legend_text += "🔵 Fort"
            elif max_precip >= 2.5:
                legend_text += "🟦 Modéré"
            elif max_precip >= 1.0:
                legend_text += "💙 Faible"
            else:
                legend_text += "💧 Bruine"
            
            ax2.text(0.02, 0.95, legend_text, transform=ax2.transAxes,
                    fontsize=10, verticalalignment='top',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
        
        # Ligne probabilité avec style moderne
        ax2_twin = ax2.twinx()
        ax2_twin.plot(temps, forecast_df['precipitation_probability'],
                     color='#00b894', linewidth=3, marker='o', markersize=4,
                     alpha=0.9, label='Probabilité pluie', markerfacecolor='white',
                     markeredgecolor='#00b894', markeredgewidth=2)
        
        ax2.set_ylabel('💧 Précipitations (mm)', color='#2c3e50')
        ax2_twin.set_ylabel('☔ Probabilité (%)', color='#00b894')
        ax2.set_xlabel('🕐 Heure locale', color='#2c3e50')
        
        # Style des axes
        for ax in [ax1, ax2, ax2_twin]:
            ax.tick_params(colors='#34495e')
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False) if ax != ax2_twin else None
            ax.spines['left'].set_color('#bdc3c7')
            ax.spines['bottom'].set_color('#bdc3c7')
        
        # Formatage des axes temporels
        for ax in [ax1, ax2]:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=3))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        # Légendes
        lines1, labels1 = ax2.get_legend_handles_labels()
        lines2, labels2 = ax2_twin.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        
        plt.tight_layout()
        
        # Ajouter des informations textuelles
        temp_moy = forecast_df['temperature_2m'].mean()
        precip_total = forecast_df['precipitation'].sum()
        
        caption = (f"🔮 Prévisions 24h pour {VILLE}\n"
                  f"🌡️ Température moyenne: {temp_moy:.1f}°C\n"
                  f"🌧️ Précipitations totales prévues: {precip_total:.1f}mm")
        
        await send_graph(message.chat.id, fig, caption)
        
    except Exception as e:
        await log_message(f"Erreur dans forecastgraph_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply("Erreur lors de la génération du graphique de prévisions.")

@router.message(Command("graph"))
async def get_graph_data_command(message: types.Message):
    """Génère 2 graphiques pour une métrique spécifique (courbe + barres). Usage: /graph temperature"""
    try:
        # Parser les arguments
        args = message.text.split()
        if len(args) not in [2, 3]:
            await message.reply(
                "Usage: /graph <métrique> [jours]\n"
                "Métriques: temperature, rain, wind, pressure, uv, humidity\n"
                "Exemple: /graph temperature 30"
            )
            return
        
        metric_arg = args[1].lower()
        days = int(args[2]) if len(args) == 3 else 30
        
        if days <= 0 or days > 365:
            await message.reply("Le nombre de jours doit être entre 1 et 365.")
            return
        
        # Mapping des arguments
        metric_mapping = {
            'temperature': 'temperature_2m',
            'temp': 'temperature_2m',
            'rain': 'precipitation',
            'precipitation': 'precipitation',
            'wind': 'windspeed_10m',
            'pressure': 'pressure_msl',
            'uv': 'uv_index',
            'humidity': 'relativehumidity_2m'
        }
        
        if metric_arg not in metric_mapping:
            await message.reply(
                f"Métrique '{metric_arg}' non reconnue.\n"
                f"Utilisez: temperature, rain, wind, pressure, uv, humidity"
            )
            return
        
        column_name = metric_mapping[metric_arg]
        metric_info = get_metric_info(column_name)
        
        # Lire et filtrer les données
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await message.reply("Aucune donnée météo disponible.")
            return
        
        df = pd.read_csv(csv_filename)
        if df.empty:
            await message.reply("Aucune donnée disponible.")
            return
        
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)
        
        # Filtrer pour ne garder que les mois complets (incluant le mois en cours) pour cette métrique
        df_complete = filter_complete_months_only(df, exclude_current_month=False, metric_column=column_name)
        
        if column_name not in df_complete.columns:
            await message.reply(f"Données pour {metric_info['name']} non disponibles.")
            return
        
        # Filtrer les derniers N jours
        now = pd.Timestamp.now(tz='UTC')
        start_date = now - pd.Timedelta(days=days)
        df_period = df_complete[(df_complete['time'] >= start_date) & (df_complete['time'] <= now)].copy()
        
        if df_period.empty:
            await message.reply(f"Aucune donnée disponible pour les {days} derniers jours.")
            return
        
        # Nettoyer les données
        df_period = df_period.dropna(subset=[column_name])
        if df_period.empty:
            await message.reply(f"Aucune donnée valide pour {metric_info['name']} sur cette période.")
            return
        
        # Convertir en heure locale
        df_period['local_time'] = df_period['time'].dt.tz_convert('Europe/Berlin')
        
        # Couleurs modernes selon la métrique
        colors = {
            'temperature_2m': '#e74c3c',      # Rouge moderne
            'precipitation': '#3498db',       # Bleu moderne
            'windspeed_10m': '#2ecc71',      # Vert moderne
            'pressure_msl': '#9b59b6',       # Violet moderne
            'uv_index': '#f39c12',           # Orange moderne
            'relativehumidity_2m': '#1abc9c' # Turquoise moderne
        }
        
        color = colors.get(column_name, '#34495e')
        
        # Statistiques communes
        data_values = df_period[column_name]
        mean_val = data_values.mean()
        max_val = data_values.max()
        min_val = data_values.min()
        
        base_caption = (f"{metric_info['emoji']} {metric_info['name']} - {days} derniers jours\n"
                       f"📊 Moyenne: {mean_val:.1f}{metric_info['unit']}\n"
                       f"📈 Maximum: {max_val:.1f}{metric_info['unit']}\n"
                       f"📉 Minimum: {min_val:.1f}{metric_info['unit']}")
        
        # === GRAPHIQUE 1: COURBE (style actuel) ===
        fig1, ax1 = plt.subplots(1, 1, figsize=(14, 8))
        
        if metric_arg in ['rain', 'precipitation']:
            # Graphique en barres pour les précipitations avec gradient MeteoSuisse
            def get_precipitation_color(precip_mm):
                """Couleurs graduées selon l'intensité des précipitations (style MeteoSuisse)."""
                if precip_mm < 0.1:
                    return '#f8f9fa'      # Blanc/transparent (pas de pluie)
                elif precip_mm < 1.0:
                    return '#cce7ff'      # Bleu très clair (bruine)
                elif precip_mm < 2.5:
                    return '#74b9ff'      # Bleu clair (faible)
                elif precip_mm < 5.0:
                    return '#4a90e2'      # Bleu moyen (modéré)
                elif precip_mm < 10.0:
                    return '#2563eb'      # Bleu foncé (fort)
                elif precip_mm < 25.0:
                    return '#7c3aed'      # Violet (très fort)
                elif precip_mm < 50.0:
                    return '#dc2626'      # Rouge (intense)
                else:
                    return '#991b1b'      # Rouge foncé (extrême)
            
            colors_precip = [get_precipitation_color(p) for p in df_period[column_name]]
            bars = ax1.bar(df_period['local_time'], df_period[column_name],
                          width=0.02, color=colors_precip, alpha=0.9, edgecolor='white', linewidth=0.5)
            
            # Ajouter légende des couleurs si précipitations significatives
            max_precip = df_period[column_name].max()
            if max_precip > 0.1:
                legend_text = "Intensité max: "
                if max_precip >= 25.0:
                    legend_text += "🔴 Extrême"
                elif max_precip >= 10.0:
                    legend_text += "🟣 Très fort"
                elif max_precip >= 5.0:
                    legend_text += "🔵 Fort"
                elif max_precip >= 2.5:
                    legend_text += "🟦 Modéré"
                elif max_precip >= 1.0:
                    legend_text += "💙 Faible"
                else:
                    legend_text += "💧 Bruine"
                
                ax1.text(0.02, 0.95, legend_text, transform=ax1.transAxes,
                        fontsize=10, verticalalignment='top',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
        else:
            # Graphique en ligne moderne avec zone remplie
            ax1.fill_between(df_period['local_time'], df_period[column_name],
                            alpha=0.3, color=color, interpolate=True)
            ax1.plot(df_period['local_time'], df_period[column_name],
                    color=color, linewidth=3, alpha=0.9, marker='o',
                    markersize=2, markerfacecolor='white', markeredgecolor=color)
            
            # Moyenne mobile avec style moderne
            if len(df_period) > 24:
                df_period['moving_avg'] = df_period[column_name].rolling(window=24, center=True).mean()
                ax1.plot(df_period['local_time'], df_period['moving_avg'],
                        color='#2c3e50', linewidth=3, alpha=0.8,
                        linestyle='--', label='📈 Moyenne mobile 24h')
                ax1.legend(loc='upper right', frameon=True, shadow=True,
                          fancybox=True, framealpha=0.9)
        
        # Titre et style pour graphique 1
        ax1.set_title(f'📊 {metric_info["emoji"]} {metric_info["name"]} (courbe) - {days} derniers jours à {VILLE}',
                     fontsize=16, fontweight='bold', color='#2c3e50', pad=20)
        ax1.set_ylabel(f'{metric_info["emoji"]} {metric_info["name"]} ({metric_info["unit"]})',
                      fontsize=12, color='#2c3e50')
        ax1.set_xlabel('📅 Date', fontsize=12, color='#2c3e50')
        
        # Style moderne des axes
        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        ax1.spines['left'].set_color('#bdc3c7')
        ax1.spines['bottom'].set_color('#bdc3c7')
        ax1.tick_params(colors='#34495e')
        
        # Formatage temporal et grille
        if days <= 7:
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m\n%H:%M'))
            ax1.xaxis.set_major_locator(mdates.HourLocator(interval=12))
        elif days <= 30:
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
            ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
        else:
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
            ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, fontsize=10)
        ax1.grid(True, alpha=0.3, linestyle=':', linewidth=0.5)
        plt.tight_layout()
        
        # Envoyer le premier graphique (courbe)
        caption1 = f"📊 Graphique en courbe\n{base_caption}"
        await send_graph(message.chat.id, fig1, caption1)
        
        # === GRAPHIQUE 2: BARRES VERTICALES (style sunshine - par mois/année) ===
        fig2, ax2 = plt.subplots(1, 1, figsize=(16, 10))
        fig2.patch.set_facecolor('#f8f9fa')
        
        # Préparer données mensuelles groupées par année (style sunshine)
        df_bars = df_period.copy()
        df_bars['year'] = df_bars['local_time'].dt.year
        df_bars['month'] = df_bars['local_time'].dt.month
        
        # Calculer valeurs mensuelles selon le type de métrique
        if metric_arg in ['rain', 'precipitation']:
            monthly_data = df_bars.groupby(['year', 'month'])[column_name].sum().reset_index()
        else:
            monthly_data = df_bars.groupby(['year', 'month'])[column_name].mean().reset_index()
        
        if monthly_data.empty:
            # Fallback vers données journalières si pas assez pour mensuel
            df_bars['date'] = df_bars['local_time'].dt.date
            if metric_arg in ['rain', 'precipitation']:
                daily_data = df_bars.groupby('date')[column_name].sum().reset_index()
            else:
                daily_data = df_bars.groupby('date')[column_name].mean().reset_index()
            
            daily_data['datetime'] = pd.to_datetime(daily_data['date'])
            bars = ax2.bar(daily_data['datetime'], daily_data[column_name],
                          width=0.8, color=color, alpha=0.8, edgecolor='white', linewidth=1)
            
            ax2.set_title(f'📊 {metric_info["emoji"]} {metric_info["name"]} (barres journalières) - {days} derniers jours à {VILLE}',
                         fontsize=16, fontweight='bold', color='#2c3e50', pad=20)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, days//15)))
        else:
            # Style sunshine : barres groupées par année pour chaque mois
            available_years = sorted(monthly_data['year'].unique())
            available_months = sorted(monthly_data['month'].unique())
            
            # Noms des mois
            month_names = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
                          'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']
            
            # Couleurs pour les années
            year_colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6']
            
            # Créer les barres groupées
            x_positions = np.arange(len(available_months))
            n_years = len(available_years)
            width = 0.8 / n_years
            
            for i, year in enumerate(available_years):
                year_data = monthly_data[monthly_data['year'] == year]
                values = []
                
                for month in available_months:
                    month_row = year_data[year_data['month'] == month]
                    if not month_row.empty:
                        values.append(month_row[column_name].iloc[0])
                    else:
                        values.append(0)
                
                year_color = year_colors[i % len(year_colors)]
                offset = (i - n_years/2 + 0.5) * width
                
                bars = ax2.bar(x_positions + offset, values, width,
                              label=f'📅 {year}', color=year_color,
                              alpha=0.8, edgecolor='white', linewidth=1)
                
                # Ajouter valeurs sur les barres
                for j, bar in enumerate(bars):
                    height = bar.get_height()
                    if height > 0:
                        ax2.text(bar.get_x() + bar.get_width()/2, height + max(values)*0.01,
                               f'{height:.0f}', ha='center', va='bottom',
                               fontsize=8, fontweight='bold', color=year_color)
            
            # Configuration des axes pour style mensuel
            ax2.set_xticks(x_positions)
            ax2.set_xticklabels([month_names[m-1] for m in available_months])
            ax2.set_title(f'📊 {metric_info["emoji"]} {metric_info["name"]} (barres mensuelles) - {len(available_years)} années à {VILLE}',
                         fontsize=16, fontweight='bold', color='#2c3e50', pad=20)
            
            # Légende moderne
            legend = ax2.legend(bbox_to_anchor=(1.02, 1), loc='upper left',
                               frameon=True, shadow=True, fancybox=True,
                               framealpha=0.95, edgecolor='#bdc3c7')
            legend.get_frame().set_facecolor('#ffffff')
        
        # Style commun des axes
        ax2.set_ylabel(f'{metric_info["emoji"]} {metric_info["name"]} ({metric_info["unit"]})',
                      fontsize=14, color='#2c3e50', fontweight='bold')
        ax2.set_xlabel('📅 Période', fontsize=14, color='#2c3e50', fontweight='bold')
        
        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        ax2.spines['left'].set_color('#bdc3c7')
        ax2.spines['bottom'].set_color('#bdc3c7')
        ax2.tick_params(colors='#34495e', labelsize=11)
        
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, fontsize=10)
        ax2.grid(True, alpha=0.3, linestyle=':', linewidth=1, axis='y')
        plt.tight_layout()
        
        # Envoyer le deuxième graphique (barres)
        caption2 = f"📊 Graphique en barres\n{base_caption}"
        await send_graph(message.chat.id, fig2, caption2)
        
    except ValueError as e:
        await message.reply("Le nombre de jours doit être un nombre entier valide.")
    except Exception as e:
        await log_message(f"Erreur dans graphdata_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply("Erreur lors de la génération des graphiques.")

@router.message(Command("heatmap"))
async def get_heatmap_command(message: types.Message):
    """Génère une heatmap calendaire des températures. Usage: /heatmap [année]"""
    try:
        # Parser les arguments
        args = message.text.split()
        current_year = pd.Timestamp.now(tz='UTC').year
        
        # Gestion du paramètre "all"
        if len(args) == 2 and args[1].lower() == 'all':
            target_year = 'all'
        elif len(args) == 2:
            try:
                target_year = int(args[1])
                if target_year < 2020 or target_year > current_year + 1:
                    await message.reply(f"Année invalide. Utilisez une année entre 2020 et {current_year}, ou 'all' pour toutes les années.")
                    return
            except ValueError:
                await message.reply(f"Format invalide. Utilisez: /heatmap [année] ou /heatmap all")
                return
        else:
            target_year = current_year
        
        # Lire les données
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await message.reply("Aucune donnée météo disponible.")
            return
        
        df = pd.read_csv(csv_filename)
        if df.empty:
            await message.reply("Aucune donnée disponible.")
            return
        
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)
        
        # Filtrer pour ne garder que les mois complets (incluant le mois en cours) pour la température
        df_complete = filter_complete_months_only(df, exclude_current_month=False, metric_column='temperature_2m')
        
        if 'temperature_2m' not in df_complete.columns:
            await message.reply("Données de température non disponibles.")
            return
        
        if target_year == 'all':
            # Mode multi-années : toutes les années disponibles
            df_complete['local_time'] = df_complete['time'].dt.tz_convert('Europe/Berlin')
            df_complete['date'] = df_complete['local_time'].dt.date
            df_complete['year'] = df_complete['local_time'].dt.year
            
            available_years = sorted(df_complete['year'].unique())
            if len(available_years) == 0:
                await message.reply("Aucune donnée disponible.")
                return
                
            # Calculer moyennes journalières par année
            daily_temps = df_complete.groupby(['year', 'date'])['temperature_2m'].mean().reset_index()
            
            if daily_temps.empty:
                await message.reply("Aucune donnée de température valide.")
                return
            
            # Créer une heatmap multi-années (années x jours de l'année)
            num_years = len(available_years)
            temp_matrix = np.full((366, num_years), np.nan)  # 366 jours max, n années
            
            for year_idx, year in enumerate(available_years):
                year_data = daily_temps[daily_temps['year'] == year]
                for _, row in year_data.iterrows():
                    day_of_year = pd.to_datetime(row['date']).timetuple().tm_yday - 1  # 0-indexé
                    if 0 <= day_of_year < 366:
                        temp_matrix[day_of_year, year_idx] = row['temperature_2m']
                        
            # Configuration pour mode multi-années
            matrix_to_plot = temp_matrix.T  # Transposer pour avoir années en Y
            xlabel_text = '📅 Jour de l\'année'
            ylabel_text = '📅 Année'
            title_text = f'🌡️ Calendrier thermique multi-années - {VILLE}\n📊 {len(available_years)} années de données'
            
            # Positions et labels pour l'axe X (jours de l'année)
            month_starts = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
            month_names = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
                          'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']
            x_positions = month_starts
            x_labels = month_names
            
            # Positions et labels pour l'axe Y (années)
            y_positions = range(len(available_years))
            y_labels = [f'📅 {year}' for year in available_years]
            
        else:
            # Mode année unique (code existant)
            df_year = df_complete[df_complete['time'].dt.year == target_year].copy()
            if df_year.empty:
                await message.reply(f"Aucune donnée disponible pour l'année {target_year}.")
                return
            
            # Convertir en heure locale et calculer moyennes journalières
            df_year['local_time'] = df_year['time'].dt.tz_convert('Europe/Berlin')
            df_year['date'] = df_year['local_time'].dt.date
            daily_temps = df_year.groupby('date')['temperature_2m'].mean().reset_index()
            
            if daily_temps.empty:
                await message.reply(f"Aucune donnée de température valide pour {target_year}.")
                return
            
            # Créer une série complète pour l'année
            start_date = datetime.date(target_year, 1, 1)
            end_date = datetime.date(target_year, 12, 31)
            all_dates = pd.date_range(start_date, end_date, freq='D').date
            
            # Créer un DataFrame complet avec NaN pour les jours manquants
            full_year = pd.DataFrame({'date': all_dates})
            full_year = full_year.merge(daily_temps, on='date', how='left')
            
            # Préparer les données pour la heatmap
            full_year['week'] = pd.to_datetime(full_year['date']).dt.isocalendar().week
            full_year['weekday'] = pd.to_datetime(full_year['date']).dt.weekday
            full_year['month'] = pd.to_datetime(full_year['date']).dt.month
            
            # Créer la matrice pour la heatmap (53 semaines x 7 jours)
            temp_matrix = np.full((53, 7), np.nan)
            
            for _, row in full_year.iterrows():
                week = int(row['week']) - 1  # 0-indexé
                weekday = int(row['weekday'])  # 0=lundi, 6=dimanche
                if 0 <= week < 53 and 0 <= weekday < 7:
                    temp_matrix[week, weekday] = row['temperature_2m']
                    
            # Configuration pour mode année unique
            matrix_to_plot = temp_matrix.T
            xlabel_text = '📅 Mois'
            ylabel_text = '📋 Jour de la semaine'
            title_text = f'🌡️ Calendrier thermique {target_year} - {VILLE}'
            
            # Labels des jours avec emojis
            y_positions = range(7)
            y_labels = ['🌅 Lun', '🌅 Mar', '🌅 Mer', '🌅 Jeu', '🌅 Ven', '🏖️ Sam', '🏖️ Dim']
            
            # Labels des mois avec emojis saisonniers
            month_positions = []
            month_labels = []
            month_emojis = ['❄️', '❄️', '🌸', '🌸', '🌸', '☀️', '☀️', '☀️', '🍂', '🍂', '🍂', '❄️']
            
            for month in range(1, 13):
                first_day_month = datetime.date(target_year, month, 1)
                week_num = first_day_month.isocalendar().week - 1
                if week_num < 53:
                    month_positions.append(week_num)
                    month_name = first_day_month.strftime('%b')
                    month_labels.append(f'{month_emojis[month-1]} {month_name}')
            
            x_positions = month_positions
            x_labels = month_labels
        
        # Créer le graphique moderne style GitHub
        if target_year == 'all':
            fig, ax = plt.subplots(1, 1, figsize=(18, 12))
        else:
            fig, ax = plt.subplots(1, 1, figsize=(16, 10))
        fig.patch.set_facecolor('#f8f9fa')
        
        # Calculer vmin/vmax pour la palette de couleurs
        if target_year == 'all':
            vmin, vmax = np.nanmin(temp_matrix), np.nanmax(temp_matrix)
        else:
            vmin, vmax = full_year['temperature_2m'].min(), full_year['temperature_2m'].max()
        
        # Heatmap moderne avec palette améliorée
        im = ax.imshow(matrix_to_plot, cmap='RdYlBu_r', aspect='auto',
                      vmin=vmin, vmax=vmax, interpolation='nearest')
        
        # Titre moderne avec style GitHub
        ax.set_title(title_text, fontsize=20, fontweight='bold', pad=30, color='#24292e')
        
        # Configuration des axes selon le mode
        ax.set_yticks(y_positions)
        ax.set_yticklabels(y_labels, fontsize=11, color='#586069')
        ax.set_xticks(x_positions)
        ax.set_xticklabels(x_labels, fontsize=11, color='#586069')
        ax.set_xlabel(xlabel_text, fontsize=14, color='#24292e', fontweight='bold')
        ax.set_ylabel(ylabel_text, fontsize=14, color='#24292e', fontweight='bold')
        
        # Colorbar moderne
        cbar = plt.colorbar(im, ax=ax, shrink=0.8, aspect=25, pad=0.02)
        cbar.set_label('🌡️ Température (°C)', fontsize=12, color='#24292e',
                      rotation=270, labelpad=20)
        cbar.ax.tick_params(labelsize=10, colors='#586069')
        
        # Style moderne des bordures
        for spine in ax.spines.values():
            spine.set_color('#d1d5da')
            spine.set_linewidth(1)
        
        ax.tick_params(colors='#586069', length=0)
        
        # Grille subtile style GitHub (adaptée selon le mode)
        if target_year == 'all':
            ax.set_xticks(np.arange(-0.5, 366, 30), minor=True)
            ax.set_yticks(np.arange(-0.5, len(available_years), 1), minor=True)
        else:
            ax.set_xticks(np.arange(-0.5, 53, 1), minor=True)
            ax.set_yticks(np.arange(-0.5, 7, 1), minor=True)
        ax.grid(which='minor', color='#e1e4e8', linestyle='-', linewidth=0.5, alpha=0.8)
        
        plt.tight_layout()
        
        # Statistiques adaptées selon le mode
        if target_year == 'all':
            # Statistiques pour mode multi-années
            valid_temps = df['temperature_2m'].dropna()
            if not valid_temps.empty:
                temp_min = valid_temps.min()
                temp_max = valid_temps.max()
                temp_mean = valid_temps.mean()
                data_points = len(valid_temps)
                
                caption = (f"🌡️ Calendrier thermique multi-années - {VILLE}\n"
                          f"📊 {len(available_years)} années • {data_points} points de données\n"
                          f"📊 Température moyenne: {temp_mean:.1f}°C\n"
                          f"📈 Maximum absolu: {temp_max:.1f}°C\n"
                          f"📉 Minimum absolu: {temp_min:.1f}°C")
            else:
                caption = f"🌡️ Calendrier thermique multi-années - {VILLE}\n❌ Aucune donnée valide"
        else:
            # Statistiques pour mode année unique
            valid_temps = full_year['temperature_2m'].dropna()
            if not valid_temps.empty:
                temp_min = valid_temps.min()
                temp_max = valid_temps.max()
                temp_mean = valid_temps.mean()
                data_coverage = len(valid_temps) / len(full_year) * 100
                
                caption = (f"🌡️ Calendrier thermique {target_year} - {VILLE}\n"
                          f"📊 Température moyenne: {temp_mean:.1f}°C\n"
                          f"📈 Maximum: {temp_max:.1f}°C\n"
                          f"📉 Minimum: {temp_min:.1f}°C\n"
                          f"📅 Couverture données: {data_coverage:.1f}%")
            else:
                caption = f"🌡️ Calendrier thermique {target_year} - {VILLE}\n❌ Aucune donnée valide"
        
        await send_graph(message.chat.id, fig, caption)
        
    except ValueError as e:
        await message.reply("L'année doit être un nombre entier valide.")
    except Exception as e:
        await log_message(f"Erreur dans heatmap_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply("Erreur lors de la génération du calendrier thermique.")

@router.message(Command("sunshinelist"))
async def get_sunshine_list_command(message: types.Message):
    """Affiche un résumé textuel mensuel de l'ensoleillement. Usage: /sunshinelist"""
    try:
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await message.reply("Aucune donnée disponible pour calculer l'ensoleillement.")
            return
            
        df = pd.read_csv(csv_filename)
        if df.empty:
            await message.reply("Aucune donnée disponible pour calculer l'ensoleillement (fichier vide).")
            return

        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)

        if df.empty:
            await message.reply("Aucune donnée temporelle valide pour calculer l'ensoleillement.")
            return

        # calculate_monthly_sunshine fait déjà le filtrage des mois complets
        monthly_sunshine = calculate_monthly_sunshine(df)
        
        if monthly_sunshine.empty:
            await message.reply(f"Pas encore de données d'ensoleillement calculées pour {VILLE}.")
            return

        # Trier par date (l'index est déjà YYYY-MM, donc tri alphabétique fonctionne)
        monthly_sunshine = monthly_sunshine.sort_index()
        
        # Détecter le premier mois avec des données (peut être partiel)
        first_month = monthly_sunshine.index[0] if not monthly_sunshine.empty else None
        current_month_year_str = pd.Timestamp.now(tz='Europe/Berlin').strftime('%Y-%m')
        
        response = f"☀️ Résumé mensuel de l'ensoleillement estimé pour {VILLE} :\n"
        response += f"📅 Début des mesures : {first_month} (données possiblement partielles)\n\n"
        
        for i, (date_str, hours) in enumerate(monthly_sunshine.items()):
            # Convertir YYYY-MM en nom de mois et année pour affichage
            try:
                month_display = pd.to_datetime(date_str + "-01").strftime('%B %Y')
            except ValueError:
                month_display = date_str # Fallback si le format est inattendu
            
            is_current = date_str == current_month_year_str
            is_first = i == 0
            
            if is_current:
                month_marker = "📍 "
            elif is_first:
                month_marker = "🚀 "  # Premier mois
            else:
                month_marker = "📅 "
            
            response += f"{month_marker}{month_display}: {hours:.1f} heures\n"
        
        await message.reply(response)
        
    except pd.errors.EmptyDataError:
        await message.reply("Le fichier de données est vide, impossible de calculer l'ensoleillement.")
    except FileNotFoundError:
        await message.reply("Fichier de données non trouvé pour calculer l'ensoleillement.")
    except Exception as e:
        await log_message(f"Error in get_sunshine_compare_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply("Erreur lors de l'obtention du résumé de l'ensoleillement.")

@router.message(Command("yearcompare"))
async def get_year_compare_command(message: types.Message):
    """Compare les données météo entre différentes années. Usage: /yearcompare [métrique]"""
    try:
        # Parser les arguments
        args = message.text.split()
        metric_arg = args[1].lower() if len(args) == 2 else 'temperature'
        
        # Mapping des métriques
        metric_mapping = {
            'temperature': 'temperature_2m',
            'temp': 'temperature_2m',
            'rain': 'precipitation',
            'precipitation': 'precipitation',
            'wind': 'windspeed_10m',
            'pressure': 'pressure_msl',
            'uv': 'uv_index',
            'humidity': 'relativehumidity_2m'
        }
        
        if metric_arg not in metric_mapping:
            await message.reply(
                f"Métrique '{metric_arg}' non reconnue.\n"
                f"Utilisez: temperature, rain, wind, pressure, uv, humidity\n"
                f"Exemple: /yearcompare temperature"
            )
            return
        
        column_name = metric_mapping[metric_arg]
        metric_info = get_metric_info(column_name)
        
        # Lire les données
        if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
            await message.reply("Aucune donnée météo disponible.")
            return
        
        df = pd.read_csv(csv_filename)
        if df.empty:
            await message.reply("Aucune donnée disponible.")
            return
        
        df['time'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df.dropna(subset=['time'], inplace=True)
        
        if column_name not in df.columns:
            await message.reply(f"Données pour {metric_info['name']} non disponibles.")
            return
        
        # Convertir en heure locale et extraire date/heure AVANT le filtrage
        df['local_time'] = df['time'].dt.tz_convert('Europe/Berlin')
        df['year'] = df['local_time'].dt.year
        df['month'] = df['local_time'].dt.month
        df['month_day'] = df['local_time'].dt.strftime('%m-%d')
        df['day_of_year'] = df['local_time'].dt.dayofyear
        
        # Filtrer pour ne garder que les mois complets (incluant le mois en cours) pour cette métrique
        df_complete = filter_complete_months_only(df, exclude_current_month=False, metric_column=column_name)
        df = df_complete.copy()
        await log_message("Filtrage automatique des mois incomplets pour /yearcompare")
        
        # Recréer les colonnes temporelles car filter_complete_months_only les supprime
        df['local_time'] = df['time'].dt.tz_convert('Europe/Berlin')
        df['year'] = df['local_time'].dt.year
        df['month'] = df['local_time'].dt.month
        df['month_day'] = df['local_time'].dt.strftime('%m-%d')
        df['day_of_year'] = df['local_time'].dt.dayofyear
        
        # Vérifier qu'on a au moins une année de données
        available_years = sorted(df['year'].unique())
        if len(available_years) < 1:
            await message.reply("Aucune année de données complète disponible.")
            return
        
        # Adapter le message selon le nombre d'années
        if len(available_years) == 1:
            await log_message(f"Affichage d'une seule année ({available_years[0]}) pour /yearcompare")
        
        current_year = pd.Timestamp.now(tz='Europe/Berlin').year
        current_day_of_year = pd.Timestamp.now(tz='Europe/Berlin').dayofyear
        
        # Calculer moyennes journalières par année
        daily_means = df.groupby(['year', 'day_of_year'])[column_name].mean().reset_index()
        
        # === GRAPHIQUE 1: COURBE DE COMPARAISON ANNUELLE ===
        fig1, ax1 = plt.subplots(1, 1, figsize=(16, 10))
        fig1.patch.set_facecolor('#f8f9fa')
        
        # Palette de couleurs moderne et distincte
        modern_colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6',
                        '#1abc9c', '#34495e', '#e67e22', '#95a5a6', '#16a085']
        
        # Tracer chaque année avec style moderne
        for i, year in enumerate(available_years):
            year_data = daily_means[daily_means['year'] == year]
            
            if not year_data.empty:
                color = modern_colors[i % len(modern_colors)]
                
                # Style spécial pour l'année courante
                if year == current_year:
                    ax1.plot(year_data['day_of_year'], year_data[column_name],
                           color=color, linewidth=4, label=f'📍 {year} (actuelle)',
                           alpha=0.95, zorder=5, marker='o', markersize=3,
                           markevery=30)
                else:
                    ax1.plot(year_data['day_of_year'], year_data[column_name],
                           color=color, linewidth=2.5, label=f'📅 {year}',
                           alpha=0.8, zorder=3)
        
        # Ligne verticale moderne pour "aujourd'hui"
        ax1.axvline(x=current_day_of_year, color='#e74c3c', linestyle='--',
                  linewidth=3, alpha=0.9, zorder=10,
                  label=f"📍 Aujourd'hui (jour {current_day_of_year})")
        
        # Zone d'intérêt moderne avec gradient
        highlight_start = max(1, current_day_of_year - 15)
        highlight_end = min(366, current_day_of_year + 15)
        ax1.axvspan(highlight_start, highlight_end, alpha=0.15, color='#e74c3c',
                  label='🔍 Période actuelle ±15j', zorder=1)
        
        # Titre moderne avec emojis
        ax1.set_title(f'{metric_info["emoji"]} Comparaison annuelle (courbe) - {metric_info["name"]} à {VILLE}\n'
                    f'📊 Analyse de {len(available_years)} années de données (mois complets uniquement)',
                    fontsize=18, fontweight='bold', color='#2c3e50', pad=25)
        
        # Labels modernes avec emojis
        ax1.set_xlabel('📅 Jour de l\'année', fontsize=14, color='#2c3e50', fontweight='bold')
        ax1.set_ylabel(f'{metric_info["emoji"]} {metric_info["name"]} ({metric_info["unit"]})',
                     fontsize=14, color='#2c3e50', fontweight='bold')
        
        # Style moderne des axes
        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        ax1.spines['left'].set_color('#bdc3c7')
        ax1.spines['bottom'].set_color('#bdc3c7')
        ax1.tick_params(colors='#34495e', labelsize=11)
        
        # Légende moderne
        legend = ax1.legend(bbox_to_anchor=(1.02, 1), loc='upper left',
                          frameon=True, shadow=True, fancybox=True,
                          framealpha=0.95, edgecolor='#bdc3c7')
        legend.get_frame().set_facecolor('#ffffff')
        
        # Marques des mois sur l'axe X
        month_starts = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
        month_names = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
                      'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']
        ax1.set_xticks(month_starts)
        ax1.set_xticklabels(month_names)
        
        plt.tight_layout()
        
        # Statistiques pour la période actuelle
        current_period_data = daily_means[
            (daily_means['day_of_year'] >= highlight_start) &
            (daily_means['day_of_year'] <= highlight_end)
        ]
        
        stats_by_year = {}
        for year in available_years:
            year_period = current_period_data[current_period_data['year'] == year]
            if not year_period.empty:
                stats_by_year[year] = year_period[column_name].mean()
        
        # Préparer le texte des statistiques
        stats_text = f"📊 Moyennes pour la période actuelle (±15j):\n"
        for year, avg_val in sorted(stats_by_year.items()):
            marker = " 👈" if year == current_year else ""
            stats_text += f"{year}: {avg_val:.1f}{metric_info['unit']}{marker}\n"
        
        # Tendance générale
        if len(stats_by_year) >= 3:
            years_list = list(stats_by_year.keys())
            values_list = list(stats_by_year.values())
            
            # Régression linéaire simple
            if len(values_list) > 1:
                slope = (values_list[-1] - values_list[0]) / (years_list[-1] - years_list[0])
                trend = "↗️ hausse" if slope > 0 else "↘️ baisse" if slope < 0 else "➡️ stable"
                stats_text += f"\n📈 Tendance générale: {trend} ({slope:.2f}{metric_info['unit']}/an)"
        
        base_caption = (f"{metric_info['emoji']} Comparaison {metric_info['name']} - {len(available_years)} années\n"
                       f"📅 Années: {min(available_years)}-{max(available_years)} (mois complets uniquement)\n"
                       f"{stats_text}")
        
        # Envoyer le premier graphique (courbe)
        caption1 = f"📊 Comparaison annuelle (courbe)\n{base_caption}"
        await send_graph(message.chat.id, fig1, caption1)
        
        # === GRAPHIQUE 2: BARRES MENSUELLES PAR ANNÉE ===
        fig2, ax2 = plt.subplots(1, 1, figsize=(16, 10))
        fig2.patch.set_facecolor('#f8f9fa')
        
        # Calculer valeurs mensuelles selon le type de métrique
        if metric_arg in ['rain', 'precipitation']:
            monthly_data = df.groupby(['year', 'month'])[column_name].sum().reset_index()
        else:
            monthly_data = df.groupby(['year', 'month'])[column_name].mean().reset_index()
        
        if not monthly_data.empty:
            # Style barres : barres groupées par année pour chaque mois
            available_years_bars = sorted(monthly_data['year'].unique())
            available_months = sorted(monthly_data['month'].unique())
            
            # Noms des mois
            month_names_bars = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
                      'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']
            
            # Couleurs pour les années (mêmes que le premier graphique)
            year_colors = modern_colors[:len(available_years_bars)]
            
            # Créer les barres groupées
            x_positions = np.arange(len(available_months))
            n_years = len(available_years_bars)
            width = 0.8 / n_years
            
            for i, year in enumerate(available_years_bars):
                year_data = monthly_data[monthly_data['year'] == year]
                values = []
                
                for month in available_months:
                    month_row = year_data[year_data['month'] == month]
                    if not month_row.empty:
                        values.append(month_row[column_name].iloc[0])
                    else:
                        values.append(0)
                
                year_color = year_colors[i % len(year_colors)]
                offset = (i - n_years/2 + 0.5) * width
                
                # Style spécial pour l'année courante
                if year == current_year:
                    bars = ax2.bar(x_positions + offset, values, width,
                                  label=f'📍 {year} (actuelle)', color=year_color,
                                  alpha=0.9, edgecolor='white', linewidth=2,
                                  zorder=5)
                    # Ajouter des valeurs sur les barres de l'année courante
                    for j, bar in enumerate(bars):
                        if values[j] > 0:
                            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01,
                                   f'{values[j]:.0f}', ha='center', va='bottom',
                                   fontsize=9, fontweight='bold', color=year_color)
                else:
                    ax2.bar(x_positions + offset, values, width,
                           label=f'📅 {year}', color=year_color,
                           alpha=0.7, edgecolor='white', linewidth=1,
                           zorder=3)
            
            # Mise en évidence du mois actuel
            current_month = pd.Timestamp.now(tz='Europe/Berlin').month
            if current_month in available_months:
                month_idx = available_months.index(current_month)
                ax2.axvspan(month_idx - 0.4, month_idx + 0.4,
                           alpha=0.2, color='#f39c12', zorder=1,
                           label=f"📍 Mois actuel ({month_names_bars[current_month-1]})")
            
            # Configuration des axes pour style mensuel
            ax2.set_xticks(x_positions)
            ax2.set_xticklabels([month_names_bars[m-1] for m in available_months])
            ax2.set_title(f'📊 {metric_info["emoji"]} {metric_info["name"]} (barres mensuelles) - {len(available_years_bars)} années à {VILLE}\n'
                         f'📊 Comparaison par mois (mois complets uniquement)',
                         fontsize=16, fontweight='bold', color='#2c3e50', pad=20)
            
            # Légende moderne
            legend = ax2.legend(bbox_to_anchor=(1.02, 1), loc='upper left',
                               frameon=True, shadow=True, fancybox=True,
                               framealpha=0.95, edgecolor='#bdc3c7')
            legend.get_frame().set_facecolor('#ffffff')
        
        # Style commun des axes
        ax2.set_ylabel(f'{metric_info["emoji"]} {metric_info["name"]} ({metric_info["unit"]})',
                      fontsize=14, color='#2c3e50', fontweight='bold')
        ax2.set_xlabel('📅 Mois', fontsize=14, color='#2c3e50', fontweight='bold')
        
        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        ax2.spines['left'].set_color('#bdc3c7')
        ax2.spines['bottom'].set_color('#bdc3c7')
        ax2.tick_params(colors='#34495e', labelsize=11)
        
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, fontsize=10)
        ax2.grid(True, alpha=0.3, linestyle=':', linewidth=1, axis='y')
        plt.tight_layout()
        
        # Envoyer le deuxième graphique (barres)
        caption2 = f"📊 Comparaison mensuelle (barres)\n{base_caption}"
        await send_graph(message.chat.id, fig2, caption2)
        
    except ValueError as e:
        await message.reply("Paramètres invalides. Vérifiez la syntaxe de la commande.")
    except Exception as e:
        await log_message(f"Erreur dans yearcompare_command: {str(e)}\n{traceback.format_exc()}")
        await message.reply("Erreur lors de la génération de la comparaison annuelle.")


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