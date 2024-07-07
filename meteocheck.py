'''
*
* PROJET : MeteoCheck
* AUTEUR : Arnaud R.
* VERSIONS : 1.4.0
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

# Configuration and setup
script_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_dir)
config_path = os.path.join(script_dir, 'config.ini')

config = configparser.ConfigParser()
config.read(config_path)

TOKEN_TELEGRAM = config['KEYS']['TELEGRAM_BOT_TOKEN']
VILLE = "Versoix"
bot = Bot(token=TOKEN_TELEGRAM)
dp = Dispatcher(bot, storage=MemoryStorage())

weather_url = "https://api.open-meteo.com/v1/forecast?latitude=46.2838&longitude=6.1621&hourly=temperature_2m,precipitation_probability,precipitation,pressure_msl,windspeed_10m,uv_index,relativehumidity_2m&timezone=GMT&forecast_days=2&past_days=2&models=best_match&timeformat=unixtime"
csv_filename = "weather_data.csv"

# Initialize CSV file if not exists
if not os.path.exists(csv_filename):
    df = pd.DataFrame(columns=['time', 'temperature_2m', 'precipitation_probability', 'precipitation', 'windspeed_10m', 'uv_index', 'pressure_msl', 'relativehumidity_2m'])
    df.to_csv(csv_filename, index=False)

# Logging functions
def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    error_message = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    asyncio.run(log_message(f"Uncaught exception: {error_message}"))

sys.excepthook = log_uncaught_exceptions

async def log_message(message: str):
    async with aiofiles.open("log_meteocheck.log", mode='a') as f:
        await f.write(f"{datetime.datetime.now(pytz.UTC)} - {message}\n")

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

async def get_weather_data():
    try:
        now = pd.Timestamp.now(tz='UTC').floor('h')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            async with session.get(weather_url) as resp:
                data = await resp.json()
                df = pd.DataFrame(data['hourly'])
                df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
                
                # Lecture du CSV existant avec un parser personnalisé
                df_existing = pd.read_csv(csv_filename)
                df_existing['time'] = pd.to_datetime(df_existing['time'], format='%Y-%m-%dT%H:%M:%SZ', utc=True)
                
                twenty_four_hours_ago = now - pd.Timedelta(hours=24)
                last_twenty_four_hours_df = df[(df['time'] >= twenty_four_hours_ago) & (df['time'] < now)]
                write_header = not os.path.exists(csv_filename)
                missing_data = last_twenty_four_hours_df[~last_twenty_four_hours_df['time'].isin(df_existing['time'])]
                if not missing_data.empty:
                    # Formater les nouvelles dates au format "2024-07-07T08:00:00Z"
                    missing_data.loc[:, 'time'] = missing_data['time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    missing_data.to_csv(csv_filename, mode='a', header=write_header, index=False)
                    await log_message(f"Enregistrement des données manquantes dans le CSV")
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
            await log_message(f"Aucune donnée obtenue à partir de get_weather_data dans check_weather! Nouvelle vérification la prochaine heure.")
            return
        for _, row in df_next_seven_hours.iterrows():
            time = row['time'].tz_convert('Europe/Berlin')
            if row['temperature_2m'] > 35 or row['temperature_2m'] < -10:
                if sent_alerts['temperature'] != time.date():
                    await send_alert(f"Alerte météo : Température prévue de {row['temperature_2m']}°C à {time} à Versoix.", row, 'temperature_2m')
                    sent_alerts['temperature'] = time.date()
            if row['precipitation_probability'] > 80 and row['precipitation'] > 15:
                if sent_alerts['precipitation'] != time.date():
                    await send_alert(f"Alerte météo : Fortes pluies prévues de {row['precipitation']}mm à {time} à Versoix.", row, 'precipitation')
                    sent_alerts['precipitation'] = time.date()
            if row['windspeed_10m'] > 60 and row['windspeed_10m'] <= 75:
                if sent_alerts['windspeed'] != time.date():
                    await send_alert(f"Alerte météo : Vent fort prévu de {row['windspeed_10m']}km/h à {time} à Versoix.", row, 'windspeed_10m')
                    sent_alerts['windspeed'] = time.date()
            if row['windspeed_10m'] > 75:
                if sent_alerts['windspeed'] != time.date():
                    await send_alert(f"Alerte météo : Vent tempétueux prévu de {row['windspeed_10m']}km/h à {time} à Versoix.", row, 'windspeed_10m')
                    sent_alerts['windspeed'] = time.date()
            if row['uv_index'] > 8:
                if sent_alerts['uv_index'] != time.date():
                    await send_alert(f"Alerte météo : Index UV prévu de {row['uv_index']} à {time} à Versoix.", row, 'uv_index')
                    sent_alerts['uv_index'] = time.date()
        if len(df_next_twenty_four_hours) >= 24:
            pressure_drop = df_next_twenty_four_hours['pressure_msl'].iloc[0] - df_next_twenty_four_hours['pressure_msl'].iloc[23]
            if pressure_drop >= 20:
                time = df_next_twenty_four_hours['time'].iloc[0].tz_convert('Europe/Berlin')
                if sent_alerts['pressure_msl'] != time.date():
                    await send_alert(f"Alerte météo : Risque de bombe météorologique détecté. Baisse de pression prévue de {round(pressure_drop, 2)} hPa sur 24 heures à partir de {time}.", df_next_twenty_four_hours.iloc[0], 'pressure_msl')
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
        await send_alert(f"Alerte météo : Nouveau record annuel possible de {alert_column} à {row[alert_column]} à {time}.")
    if alert_column == 'temperature_2m':
        min_value = df[alert_column].min()
        if row[alert_column] < min_value:
            time = row['time'].tz_convert('Europe/Berlin')
            await send_alert(f"Alerte météo : Nouveau record minimum annuel possible de {alert_column} à {row[alert_column]} à {time}.")

def generate_summary(df):
    # Assurez-vous que le DataFrame est en UTC
    df['time'] = df['time'].dt.tz_convert('UTC')
    
    # Calculez les statistiques en UTC
    df['day'] = df['time'].dt.date
    df['daily_precipitation'] = df.groupby('day')['precipitation'].transform('sum')
    
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

    # Convertir les dates en 'Europe/Berlin' uniquement pour l'affichage
    hot_day_berlin = hot_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")
    cold_day_berlin = cold_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")
    rain_day_berlin = rain_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")
    uv_day_berlin = uv_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")
    wind_day_berlin = wind_day.tz_convert('Europe/Berlin').strftime("%Y-%m-%d")

    # Créer le résumé avec les dates converties
    summary = f"Jour le plus chaud: {hot_day_berlin} ({max_temp:.1f}°C)\n"
    summary += f"Jour le plus froid: {cold_day_berlin} ({min_temp:.1f}°C)\n"
    summary += f"Jour le plus pluvieux: {rain_day_berlin} ({max_precipitation:.1f}mm)\n"
    summary += f"Jour avec l'index UV le plus élevé: {uv_day_berlin} (index {max_uv_index:.1f})\n"
    summary += f"Jour le plus venteux: {wind_day_berlin} ({max_wind_speed:.1f} km/h)\n"
    summary += f"Nombre de jours de pluie: {rainy_days}\n"
    summary += f"Température moyenne: {avg_temp:.1f}°C\n"
    summary += f"Précipitations totales: {total_precipitation:.1f}mm"
    
    return summary

@dp.message_handler(commands='start')
async def start_command(message: types.Message):
    chat_id = message.chat.id
    if os.path.exists('chat_ids.json'):
        with open('chat_ids.json', 'r') as file:
            chats = json.load(file)
        if chat_id not in chats:
            chats.append(chat_id)
            with open('chat_ids.json', 'w') as file:
                json.dump(chats, file)
    else:
        chats = [chat_id]
        with open('chat_ids.json', 'w') as file:
            json.dump(chats, file)
    await message.reply("Bot started!")

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
            
            response = f"Dernières informations météo pour {VILLE} :\n"
            response += f"Date et heure: {latest_info['time']}\n"
            response += f"Température: {latest_info['temperature_2m']}°C\n"
            response += f"Probabilité de précipitation: {latest_info['precipitation_probability']}%\n"
            response += f"Précipitation: {latest_info['precipitation']}mm\n"
            response += f"Vitesse du vent: {latest_info['windspeed_10m']}km/h\n"
            response += f"Indice UV: {latest_info['uv_index']}\n"
            response += f"Pression au niveau de la mer: {latest_info['pressure_msl']} hPa\n"
            response += f"Humidité relative: {latest_info['relativehumidity_2m']}%\n"
            
            await log_message("Réponse préparée, tentative d'envoi")
            await message.reply(response)
            await log_message("Réponse envoyée avec succès")
    except aiohttp.ClientError as e:
        await log_message(f"Erreur de connexion aiohttp: {str(e)}")
        await message.reply("Erreur de connexion lors de l'envoi de la réponse. Veuillez réessayer plus tard.")
    except Exception as e:
        await log_message(f"Error in get_latest_info_command: {str(e)}")
        await message.reply(f"Erreur lors de l'obtention des informations : {str(e)}")

@dp.message_handler(commands='month')
async def get_month_summary(message: types.Message):
    await send_month_summary(message.chat.id)

@dp.message_handler(commands='year')
async def get_year_summary(message: types.Message):
    await send_year_summary(message.chat.id)

# Main execution
if __name__ == "__main__":
    from aiogram import executor
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(schedule_jobs()) 
    executor.start_polling(dp, skip_updates=True)