'''
*
* PROJET : MeteoCheck
* AUTEUR : Arnaud R.
* VERSIONS : 1.6.4
* NOTES : None
*
'''
import os
import pandas as pd
import asyncio
from aiogram import Bot, Dispatcher, types, exceptions
import datetime
import aiofiles
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Command
import aiohttp
import sys
import traceback
import configparser
import json
import pytz
import warnings
import locale
# Pour Linux
locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8') 

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

# Utilisez ces valeurs pour construire l'URL de l'API météo
weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=temperature_2m,precipitation_probability,precipitation,pressure_msl,windspeed_10m,uv_index,relativehumidity_2m&timezone=GMT&forecast_days=2&past_days=2&models=best_match&timeformat=unixtime"

bot = Bot(token=TOKEN_TELEGRAM)
dp = Dispatcher(bot, storage=MemoryStorage())

csv_filename = "weather_data.csv"

# Initialize CSV file if not exists
if not os.path.exists(csv_filename):
    df = pd.DataFrame(columns=['time', 'temperature_2m', 'precipitation_probability', 'precipitation', 'pressure_msl', 'windspeed_10m', 'uv_index', 'relativehumidity_2m'])
    df.to_csv(csv_filename, index=False)

# Logging functions
def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    error_message = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    asyncio.run(log_message(f"Uncaught exception: {error_message}"))

sys.excepthook = log_uncaught_exceptions

async def log_message(message: str):
    async with aiofiles.open("log_meteocheck.log", mode='a') as f:
        await f.write(f"{datetime.datetime.now(pytz.UTC)} - {message}\n")

#Permet de vérifier la structure des colonnes et de time au lancement du script
def clean_csv_file():
    try:
        df = pd.read_csv(csv_filename)
        correct_columns = ['time', 'temperature_2m', 'precipitation_probability', 'precipitation', 'pressure_msl', 'windspeed_10m', 'uv_index', 'relativehumidity_2m']
        
        # Réorganiser les colonnes selon l'ordre correct
        df = df.reindex(columns=correct_columns)
        
        # Convertir la colonne time en datetime
        df['time'] = pd.to_datetime(df['time'], utc=True)
        
        # Trier par date
        df = df.sort_values('time')
        
        # Convertir toutes les dates au format "2024-07-07T05:00:00Z"
        df['time'] = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Écrire le DataFrame nettoyé dans le fichier CSV
        df.to_csv(csv_filename, index=False)
        print("Fichier CSV nettoyé avec succès.")
    except Exception as e:
        print(f"Erreur lors du nettoyage du fichier CSV : {str(e)}")

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
    while True:
        await check_weather()
        await asyncio.sleep(60)  # Attendre 60 secondes avant la prochaine exécution

async def send_alert(message, row=None, alert_column=None):
    if row is not None and alert_column is not None:
        await check_records(row, alert_column)

    try:
        with open('chat_ids.json', 'r') as file:
            chats = json.load(file)
        send_tasks = [send_message_with_retry(chat_id, message) for chat_id in chats]
        await asyncio.gather(*send_tasks)
        await log_message(f"Sent alert: {message}")
    except Exception as e:
        await log_message(f"Error in send_alert: {str(e)}")

async def start_polling(dispatcher: Dispatcher, timeout: int = 20, relax: float = 0.1, fast: bool = True):
    max_retries = 5
    for retry in range(max_retries):
        try:
            return await dispatcher.start_polling(timeout=timeout, relax=relax, fast=fast)
        except aiogram.utils.exceptions.TelegramAPIError as e:
            if "Bad Gateway" in str(e) and retry < max_retries - 1:
                await asyncio.sleep(5)  # Attendre 5 secondes avant de réessayer
            else:
                raise

async def send_message_with_retry(chat_id, message, max_retries=3):
    for attempt in range(max_retries):
        try:
            await bot.send_message(chat_id=chat_id, text=message)
            return
        except exceptions.TelegramAPIError as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
            else:
                print(f"Échec de l'envoi du message au chat {chat_id} après {max_retries} tentatives : {e}")

async def send_month_summary(chat_id):
    try:
        await log_message(f"Début de send_month_summary pour chat_id: {chat_id}")
        
        # Lecture du CSV
        df = pd.read_csv(csv_filename)
        await log_message("CSV lu avec succès")
        
        # Conversion des dates
        df['time'] = pd.to_datetime(df['time'], utc=True)
        await log_message("Conversion des dates effectuée")
        
        now = pd.Timestamp.now(tz='UTC')
        last_month = (now.replace(day=1) - pd.Timedelta(days=1)).replace(day=1)
        df = df[(df['time'] >= last_month) & (df['time'] < now.replace(day=1))]
        await log_message(f"Données filtrées pour le mois dernier: {len(df)} entrées")
        
        if df.empty:
            await log_message("Aucune donnée disponible pour le mois dernier")
            await bot.send_message(chat_id, "Pas de données disponibles pour le mois dernier.")
            return
        
        summary = generate_summary(df)
        await log_message("Résumé généré avec succès")
        
        message = f"Résumé du mois dernier pour {VILLE}:\n\n{summary}"
        await bot.send_message(chat_id, message)
        await log_message(f"Résumé mensuel envoyé avec succès à chat_id: {chat_id}")
    
    except pd.errors.EmptyDataError:
        error_msg = "Le fichier CSV est vide ou mal formaté"
        await log_message(error_msg)
        await bot.send_message(chat_id, f"Erreur : {error_msg}")
    except FileNotFoundError:
        error_msg = f"Le fichier {csv_filename} n'a pas été trouvé"
        await log_message(error_msg)
        await bot.send_message(chat_id, f"Erreur : {error_msg}")
    except aiohttp.ClientError as e:
        error_msg = f"Erreur de connexion lors de l'envoi du message: {str(e)}"
        await log_message(error_msg)
        await bot.send_message(chat_id, "Erreur de connexion. Veuillez réessayer plus tard.")
    except Exception as e:
        await log_message(f"Erreur inattendue dans send_month_summary: {str(e)}")
        await bot.send_message(chat_id, "Une erreur inattendue s'est produite lors de la génération du résumé mensuel.")

async def send_year_summary(chat_id):
    try:
        await log_message(f"Début de send_year_summary pour chat_id: {chat_id}")
        
        # Lecture du CSV
        df = pd.read_csv(csv_filename)
        await log_message("CSV lu avec succès")
        
        # Conversion des dates
        df['time'] = pd.to_datetime(df['time'], utc=True)
        await log_message("Conversion des dates effectuée")
        
        now = pd.Timestamp.now(tz='UTC')
        current_year = now.year
        df = df[(df['time'].dt.year == current_year) & (df['time'] < now)]
        await log_message(f"Données filtrées pour l'année en cours: {len(df)} entrées")
        
        if df.empty:
            await log_message("Aucune donnée disponible pour cette année")
            await bot.send_message(chat_id, "Pas de données disponibles pour cette année.")
            return
        
        summary = generate_summary(df)
        await log_message("Résumé généré avec succès")
        
        message = f"Résumé de l'année en cours pour {VILLE}:\n\n{summary}"
        await bot.send_message(chat_id, message)
        await log_message(f"Résumé annuel envoyé avec succès à chat_id: {chat_id}")
    
    except pd.errors.EmptyDataError:
        error_msg = "Le fichier CSV est vide ou mal formaté"
        await log_message(error_msg)
        await bot.send_message(chat_id, f"Erreur : {error_msg}")
    except FileNotFoundError:
        error_msg = f"Le fichier {csv_filename} n'a pas été trouvé"
        await log_message(error_msg)
        await bot.send_message(chat_id, f"Erreur : {error_msg}")
    except aiohttp.ClientError as e:
        error_msg = f"Erreur de connexion lors de l'envoi du message: {str(e)}"
        await log_message(error_msg)
        await bot.send_message(chat_id, "Erreur de connexion. Veuillez réessayer plus tard.")
    except Exception as e:
        await log_message(f"Erreur inattendue dans send_year_summary: {str(e)}")
        await bot.send_message(chat_id, "Une erreur inattendue s'est produite lors de la génération du résumé annuel.")

async def send_all_summary(chat_id):
    try:
        await log_message(f"Début de send_all_summary pour chat_id: {chat_id}")
        
        # Lecture du CSV
        df = pd.read_csv(csv_filename)
        await log_message("CSV lu avec succès")
        
        # Conversion des dates
        df['time'] = pd.to_datetime(df['time'], utc=True)
        await log_message("Conversion des dates effectuée")
        
        if df.empty:
            await log_message("Aucune donnée disponible")
            await bot.send_message(chat_id, "Pas de données disponibles.")
            return
        
        summary = generate_summary(df)
        await log_message("Résumé généré avec succès")
        
        message = f"Résumé de toutes les données météo pour {VILLE}:\n\n{summary}"
        await bot.send_message(chat_id, message)
        await log_message(f"Résumé complet envoyé avec succès à chat_id: {chat_id}")
    
    except pd.errors.EmptyDataError:
        error_msg = "Le fichier CSV est vide ou mal formaté"
        await log_message(error_msg)
        await bot.send_message(chat_id, f"Erreur : {error_msg}")
    except FileNotFoundError:
        error_msg = f"Le fichier {csv_filename} n'a pas été trouvé"
        await log_message(error_msg)
        await bot.send_message(chat_id, f"Erreur : {error_msg}")
    except aiohttp.ClientError as e:
        error_msg = f"Erreur de connexion lors de l'envoi du message: {str(e)}"
        await log_message(error_msg)
        await bot.send_message(chat_id, "Erreur de connexion. Veuillez réessayer plus tard.")
    except Exception as e:
        await log_message(f"Erreur inattendue dans send_all_summary: {str(e)}")
        await bot.send_message(chat_id, "Une erreur inattendue s'est produite lors de la génération du résumé complet.")

async def get_weather_data():
    try:
        now = pd.Timestamp.now(tz='UTC').floor('h')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            async with session.get(weather_url) as resp:
                data = await resp.json()
                columns = ['time', 'temperature_2m', 'precipitation_probability', 'precipitation', 'pressure_msl', 'windspeed_10m', 'uv_index', 'relativehumidity_2m']
                df = pd.DataFrame({col: data['hourly'][col] for col in columns})
                df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
                
                # Lecture du CSV existant
                if os.path.exists(csv_filename):
                    df_existing = pd.read_csv(csv_filename)
                    df_existing['time'] = pd.to_datetime(df_existing['time'], utc=True)
                else:
                    df_existing = pd.DataFrame(columns=columns)
                
                twenty_four_hours_ago = now - pd.Timedelta(hours=24)
                last_twenty_four_hours_df = df[(df['time'] >= twenty_four_hours_ago) & (df['time'] < now)]
                missing_data = last_twenty_four_hours_df[~last_twenty_four_hours_df['time'].isin(df_existing['time'])]
                if not missing_data.empty:
                    try:
                        missing_data = missing_data.copy()
                        missing_data['time'] = pd.to_datetime(missing_data['time']).strftime('%Y-%m-%dT%H:%M:%SZ')
                        missing_data.to_csv(csv_filename, mode='a', header=not os.path.exists(csv_filename), index=False)
                        await log_message(f"Enregistrement des données manquantes dans le CSV")
                    except Exception as e:
                        await log_message(f"Erreur lors de la conversion des dates : {str(e)}")
                
                seven_hours_later = now + pd.Timedelta(hours=7)
                next_seven_hours_df = df[(df['time'] > now) & (df['time'] <= seven_hours_later)]
                twenty_four_hours_later = now + pd.Timedelta(hours=24)
                next_twenty_four_hours_df = df[(df['time'] > now) & (df['time'] <= twenty_four_hours_later)]
                return next_seven_hours_df, next_twenty_four_hours_df
    except Exception as e:
        await log_message(f"Error in get_weather_data: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

async def check_weather():
    await log_message(f"Fonction check_weather executée")
    try:
        df_next_seven_hours, df_next_twenty_four_hours = await get_weather_data()
        if df_next_seven_hours.empty and df_next_twenty_four_hours.empty:
            await log_message(f"Aucune donnée obtenue à partir de get_weather_data dans check_weather! Nouvelle vérification dans 1 minute.")
            return
        for _, row in df_next_seven_hours.iterrows():
            time = row['time'].tz_convert('Europe/Berlin')
            if row['temperature_2m'] > 35 or row['temperature_2m'] < -10:
                if sent_alerts['temperature'] != time.date():
                    emoji = "🔥" if row['temperature_2m'] > 35 else "❄️"
                    await send_alert(f"{emoji} Alerte météo : Température prévue de {row['temperature_2m']}°C à {time} à {VILLE}.", row, 'temperature_2m')
                    sent_alerts['temperature'] = time.date()
            if row['precipitation_probability'] > 80 and row['precipitation'] > 15:
                if sent_alerts['precipitation'] != time.date():
                    await send_alert(f"🌧️ Alerte météo : Fortes pluies prévues de {row['precipitation']}mm à {time} à {VILLE}.", row, 'precipitation')
                    sent_alerts['precipitation'] = time.date()
            if row['windspeed_10m'] > 60:
                if sent_alerts['windspeed'] != time.date():
                    emoji = "🌪️" if row['windspeed_10m'] > 75 else "💨"
                    wind_type = "tempétueux" if row['windspeed_10m'] > 75 else "fort"
                    await send_alert(f"{emoji} Alerte météo : Vent {wind_type} prévu de {row['windspeed_10m']}km/h à {time} à {VILLE}.", row, 'windspeed_10m')
                    sent_alerts['windspeed'] = time.date()
            if row['uv_index'] > 8:
                if sent_alerts['uv_index'] != time.date():
                    await send_alert(f"☀️ Alerte météo : Index UV prévu de {row['uv_index']} à {time} à {VILLE}.", row, 'uv_index')
                    sent_alerts['uv_index'] = time.date()
        if len(df_next_twenty_four_hours) >= 24:
            pressure_drop = df_next_twenty_four_hours['pressure_msl'].iloc[0] - df_next_twenty_four_hours['pressure_msl'].iloc[23]
            if pressure_drop >= 20:
                time = df_next_twenty_four_hours['time'].iloc[0].tz_convert('Europe/Berlin')
                if sent_alerts['pressure_msl'] != time.date():
                    await send_alert(f"🌪️ Alerte météo : Risque de bombe météorologique détecté. Baisse de pression prévue de {round(pressure_drop, 2)} hPa sur 24 heures à partir de {time}.", df_next_twenty_four_hours.iloc[0], 'pressure_msl')
                    sent_alerts['pressure_msl'] = time.date()
    except Exception as e:
        await log_message(f"Error in check_weather: {str(e)}")

async def check_records(row, alert_column):
    df = pd.read_csv(csv_filename)
    if df.empty:
        await log_message(f"No data found in the csv file.")
        return
    df['time'] = pd.to_datetime(df['time'], utc=True)
    current_year = pd.Timestamp.now(tz='UTC').year
    df = df[df['time'].dt.year == current_year]
    max_value = df[alert_column].max()
    if row[alert_column] > max_value:
        time = row['time'].tz_convert('Europe/Berlin')
        await send_alert(f"🏆 Alerte météo : Nouveau record annuel possible de {alert_column} à {row[alert_column]} à {time}.")
    if alert_column == 'temperature_2m':
        min_value = df[alert_column].min()
        if row[alert_column] < min_value:
            time = row['time'].tz_convert('Europe/Berlin')
            await send_alert(f"🏆 Alerte météo : Nouveau record minimum annuel possible de {alert_column} à {row[alert_column]} à {time}.")

def calculate_sunshine_hours(df):
    # Convertir l'heure en heure locale
    df['local_time'] = df['time'].dt.tz_convert('Europe/Berlin')
    
    # Ajuster les heures de jour selon la saison
    # En été (juin-août)
    summer_mask = df['local_time'].dt.month.isin([6, 7, 8])
    df.loc[summer_mask, 'is_daytime'] = (
        (df.loc[summer_mask, 'local_time'].dt.hour >= 5) & 
        (df.loc[summer_mask, 'local_time'].dt.hour < 22)
    )
    
    # Au printemps (mars-mai) et automne (septembre-novembre)
    midseason_mask = df['local_time'].dt.month.isin([3, 4, 5, 9, 10, 11])
    df.loc[midseason_mask, 'is_daytime'] = (
        (df.loc[midseason_mask, 'local_time'].dt.hour >= 6) & 
        (df.loc[midseason_mask, 'local_time'].dt.hour < 21)
    )
    
    # En hiver (décembre-février)
    winter_mask = df['local_time'].dt.month.isin([12, 1, 2])
    df.loc[winter_mask, 'is_daytime'] = (
        (df.loc[winter_mask, 'local_time'].dt.hour >= 8) & 
        (df.loc[winter_mask, 'local_time'].dt.hour < 17)
    )
    
    # Considérer comme ensoleillé si :
    # - C'est le jour
    # - L'indice UV est supérieur à 2
    df['is_sunny'] = (df['is_daytime'] & (df['uv_index'] > 2))
    
    # Compter les heures ensoleillées
    sunshine_hours = df['is_sunny'].sum()
    
    return sunshine_hours

def calculate_monthly_sunshine(df):
    # Assurez-vous que la colonne 'time' est en UTC
    df['time'] = df['time'].dt.tz_convert('UTC')
    
    # Grouper par année et mois sans perdre l'information de fuseau horaire
    monthly_sunshine = df.groupby([df['time'].dt.year, df['time'].dt.month]).apply(calculate_sunshine_hours)
    
    # Créer un index plus lisible
    monthly_sunshine.index = pd.to_datetime(monthly_sunshine.index.map(lambda x: f"{x[0]}-{x[1]:02d}-01")).strftime('%Y-%m')
    
    return monthly_sunshine

def generate_summary(df):
    # Assurez-vous que le DataFrame est en UTC
    df['time'] = df['time'].dt.tz_convert('UTC')
    
    # Calculez les statistiques en UTC
    df['day'] = df['time'].dt.date
    df['daily_precipitation'] = df.groupby('day')['precipitation'].transform('sum')
    
    sunshine_hours = calculate_sunshine_hours(df)

    hot_day = df.loc[df['temperature_2m'].idxmax(), 'time']
    max_temp = df['temperature_2m'].max()
    cold_day = df.loc[df['temperature_2m'].idxmin(), 'time']
    min_temp = df['temperature_2m'].min()
    rain_day = df.loc[df['daily_precipitation'].idxmax(), 'time']
    max_precipitation = df['daily_precipitation'].max()
    uv_day = df.loc[df['uv_index'].idxmax(), 'time']
    max_uv_index = df['uv_index'].max()
    wind_day = df.loc[df['windspeed_10m'].idxmax(), 'time']
    max_wind_speed = df['windspeed_10m'].max()
    rain_threshold = 0.1
    rainy_days = df[df['daily_precipitation'] > rain_threshold]['day'].nunique()
    avg_temp = df['temperature_2m'].mean()
    total_precipitation = df['precipitation'].sum()
    humid_day = df.loc[df['relativehumidity_2m'].idxmax(), 'time']
    max_humidity = df['relativehumidity_2m'].max()
    dry_day = df.loc[df['relativehumidity_2m'].idxmin(), 'time']
    min_humidity = df['relativehumidity_2m'].min()

    # Convertir les dates en 'Europe/Berlin' uniquement pour l'affichage
    hot_day_berlin = hot_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")
    cold_day_berlin = cold_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")
    rain_day_berlin = rain_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")
    uv_day_berlin = uv_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")
    wind_day_berlin = wind_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")
    humid_day_berlin = humid_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")
    dry_day_berlin = dry_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")

    # Créer le résumé avec les dates converties
    summary = f"🌡️ Jour le plus chaud: {hot_day_berlin} ({max_temp:.1f}°C)\n"
    summary += f"❄️ Jour le plus froid: {cold_day_berlin} ({min_temp:.1f}°C)\n"
    summary += f"🌧️ Jour le plus pluvieux: {rain_day_berlin} ({max_precipitation:.1f}mm)\n"
    summary += f"☀️ Jour avec l'index UV le plus élevé: {uv_day_berlin} (index {max_uv_index:.1f})\n"
    summary += f"💨 Jour le plus venteux: {wind_day_berlin} ({max_wind_speed:.1f} km/h)\n"
    summary += f"🌂 Nombre de jours de pluie: {rainy_days}\n"
    summary += f"🌡️ Température moyenne: {avg_temp:.1f}°C\n"
    summary += f"💧 Précipitations totales: {total_precipitation:.1f}mm\n"
    summary += f"💦 Jour le plus humide: {humid_day_berlin} ({max_humidity:.1f}%)\n"
    summary += f"🏜️ Jour le plus sec: {dry_day_berlin} ({min_humidity:.1f}%)\n"
    summary += f"☀️ Heures d'ensoleillement estimées: {sunshine_hours:.1f} heures\n"

    return summary

@dp.message_handler(commands='start')
async def start_command(message: types.Message):
    chat_id = message.chat.id
    is_new_user = True
    
    if os.path.exists('chat_ids.json'):
        with open('chat_ids.json', 'r') as file:
            chats = json.load(file)
        if chat_id in chats:
            is_new_user = False
        else:
            chats.append(chat_id)
    else:
        chats = [chat_id]
    
    with open('chat_ids.json', 'w') as file:
        json.dump(chats, file)
    
    if is_new_user:
        welcome_message = (
            "Bienvenue sur le bot météo de {VILLE}! 🌤️\n\n"
            "Voici les commandes disponibles :\n"
            "/weather - Obtenir les dernières informations météo\n"
            "/forecast - Voir les prévisions pour les prochaines heures\n"
            "/sunshine - Voir le résumé mensuel de l'ensoleillement\n"
            "/month - Obtenir le résumé météo du mois dernier\n"
            "/year - Obtenir le résumé météo de l'année en cours\n"
            "/all - Obtenir le résumé météo de toutes les données disponibles\n\n"
            "N'hésitez pas à utiliser ces commandes pour rester informé sur la météo à {VILLE}!"
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

@dp.message_handler(commands='weather')
async def get_latest_info_command(message: types.Message):
    try:
        await log_message("Début de get_latest_info_command")
                
        # Lire le CSV et obtenir la dernière ligne
        df = pd.read_csv(csv_filename)
        await log_message("CSV lu avec succès")
        
        if df.empty:
            await log_message("Le DataFrame est vide")
            await message.reply("Aucune donnée disponible.")
        else:
            latest_info = df.iloc[-1].to_dict()
            latest_info['time'] = pd.to_datetime(latest_info['time'], utc=True).tz_convert('Europe/Berlin').strftime("%Y-%m-%d %H:%M:%S")
            
            response = f"🌡️ Météo actuelle à {VILLE} :\n\n"
            response += f"📅 {latest_info['time']}\n\n"
            response += f"🌡️ Température: {latest_info['temperature_2m']}°C\n"
            response += f"🌧️ Probabilité de pluie: {latest_info['precipitation_probability']}%\n"
            response += f"💧 Précipitations: {latest_info['precipitation']}mm\n"
            response += f"💨 Vent: {latest_info['windspeed_10m']}km/h\n"
            response += f"☀️ Indice UV: {latest_info['uv_index']}\n"
            response += f"🌡️ Pression: {latest_info['pressure_msl']} hPa\n"
            response += f"💦 Humidité: {latest_info['relativehumidity_2m']}%\n"
            
            await log_message("Réponse préparée, tentative d'envoi")
            await message.reply(response)
            await log_message("Réponse envoyée avec succès")
    except Exception as e:
        await log_message(f"Error in get_latest_info_command: {str(e)}")
        await message.reply(f"Erreur lors de l'obtention des informations : {str(e)}")

@dp.message_handler(commands='forecast')
async def get_forecast(message: types.Message):
    try:
        df_next_seven_hours, _ = await get_weather_data()
        if df_next_seven_hours.empty:
            await message.reply("Aucune donnée de prévision disponible.")
            return
        
        forecast = df_next_seven_hours.head(6)
        response = f"🔮 Prévisions météo pour {VILLE}\n\n"
        
        for _, row in forecast.iterrows():
            time = row['time'].tz_convert('Europe/Berlin').strftime("%H:%M")
            temp = row['temperature_2m']
            precip = row['precipitation']
            precip_prob = row['precipitation_probability']
            wind = row['windspeed_10m']
            uv = row['uv_index']
            humidity = row['relativehumidity_2m']
            
            temp_emoji = "🥵" if temp > 30 else "🥶" if temp < 10 else "🌡️"
            precip_emoji = "🌧️" if precip > 0 else "☀️"
            wind_emoji = "🌬️" if wind > 20 else "🍃"
            
            response += f"⏰ {time}:\n"
            response += f"{temp_emoji} {temp:.1f}°C | "
            response += f"{precip_emoji} {precip:.1f}mm ({precip_prob}%) | "
            response += f"{wind_emoji} {wind:.1f}km/h | "
            response += f"☀️ UV: {uv:.1f} | "
            response += f"💦 {humidity}%\n\n"
        
        await message.reply(response)
    except Exception as e:
        await log_message(f"Error in get_forecast: {str(e)}")
        await message.reply("Erreur lors de l'obtention des prévisions.")

@dp.message_handler(commands='sunshine')
async def get_sunshine_summary(message: types.Message):
    try:
        df = pd.read_csv(csv_filename)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        monthly_sunshine = calculate_monthly_sunshine(df)
        
        # Convertir l'index en datetime pour le tri
        monthly_sunshine.index = pd.to_datetime(monthly_sunshine.index + '-01')
        
        # Trier par date
        monthly_sunshine = monthly_sunshine.sort_index()
        
        # Convertir l'index en format plus lisible
        formatted_index = monthly_sunshine.index.strftime('%B %Y')  # Nom complet du mois
        
        response = "☀️ Résumé mensuel de l'ensoleillement pour {VILLE} :\n\n"
        
        # Calculer le total des heures d'ensoleillement
        total_sunshine = monthly_sunshine.sum()
        
        # Pour chaque mois, ajouter le détail à la réponse
        for date, hours in zip(formatted_index, monthly_sunshine):
            # Ajouter une indication si c'est le mois en cours
            is_current = date == pd.Timestamp.now(tz='UTC').strftime('%B %Y')
            month_marker = "📍 " if is_current else "📅 "
            response += f"{month_marker}{date}: {hours:.1f} heures\n"
        
        # Ajouter le total à la fin
        response += f"\n💡 Total: {total_sunshine:.1f} heures d'ensoleillement"
        
        await message.reply(response)
        
    except Exception as e:
        await log_message(f"Error in get_sunshine_summary: {str(e)}")
        await message.reply("Erreur lors de l'obtention du résumé de l'ensoleillement.")

@dp.message_handler(commands='month')
async def get_month_summary(message: types.Message):
    await send_month_summary(message.chat.id)

@dp.message_handler(commands='year')
async def get_year_summary(message: types.Message):
    await send_year_summary(message.chat.id)

@dp.message_handler(commands='all')
async def get_all_summary(message: types.Message):
    await send_all_summary(message.chat.id)

# Main execution
if __name__ == "__main__":
    from aiogram import executor
    clean_csv_file()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(schedule_jobs()) 
    executor.start_polling(dp, skip_updates=True, on_startup=start_polling)
