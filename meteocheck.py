'''
*
* PROJET : MeteoCheck
* AUTEUR : Arnaud R.
* VERSIONS : 1.0.0
* NOTES : None
*
'''
import os
import pandas as pd
import asyncio
from aiogram import Bot, Dispatcher, types
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

config = configparser.ConfigParser()
# read the content of the config.ini file
config.read('config.ini')  

TOKEN_TELEGRAM = config['KEYS']['TELEGRAM_BOT_TOKEN']
# Créer le bot Telegram
bot = Bot(token=TOKEN_TELEGRAM)
# Ajouté un chat_id
dp = Dispatcher(bot, storage=MemoryStorage())

# Définir l'URL de l'API météo
weather_url = "https://api.open-meteo.com/v1/forecast?latitude=46.2838&longitude=6.1621&hourly=temperature_2m,precipitation_probability,precipitation,pressure_msl,windspeed_10m,uv_index&timezone=Europe%2FBerlin&forecast_days=1&models=best_match&timeformat=unixtime"

# Définir le nom du fichier CSV où stocker les données
csv_filename = "weather_data.csv"

# Vérifiez si le fichier existe, sinon créez-le.
if not os.path.exists(csv_filename):
    # Créer un DataFrame vide avec les bonnes colonnes
    df = pd.DataFrame(columns=['time', 'temperature', 'precipitation_probability', 'precipitation', 'windspeed', 'uv_index', 'pressure_msl'])
    # Enregistrer le DataFrame vide dans un CSV
    df.to_csv(csv_filename, index=False)

# Définir les alertes déjà envoyées pour le jour en cours
sent_alerts = {
    'temperature': None,
    'precipitation': None,
    'windspeed': None,
    'uv_index': None,
    'pressure': None
}

async def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    error_message = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    await log_message(f"Uncaught exception: {error_message}")

sys.excepthook = log_uncaught_exceptions

async def log_message(message: str):
    async with aiofiles.open("log_meteocheck.log", mode='a') as f:
        await f.write(f"{datetime.datetime.now()} - {message}\n")

async def get_weather_data():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(weather_url) as resp:
                data = await resp.json()
    
                # Extraire les données nécessaires
                time = data['hourly']['time']
                temperature = data['hourly']['temperature_2m']
                precipitation_probability = data['hourly']['precipitation_probability']
                precipitation = data['hourly']['precipitation']
                windspeed = data['hourly']['windspeed_10m']
                uv_index = data['hourly']['uv_index']
                pressure_msl = data['hourly']['pressure_msl']  

                # Créer un DataFrame avec les données
                df = pd.DataFrame({
                    'time': time,
                    'temperature': temperature,
                    'precipitation_probability': precipitation_probability,
                    'precipitation': precipitation,
                    'windspeed': windspeed,
                    'uv_index': uv_index,
                    'pressure_msl': pressure_msl   
                })
                
                # Convertir le temps unix en un objet datetime pour faciliter les comparaisons
                df['time'] = pd.to_datetime(df['time'], unit='s').dt.tz_localize('Europe/Berlin')
                # Arrondir les heures dans la DataFrame
                df['time'] = df['time'].dt.floor('H')

                # Arrondir l'heure actuelle à l'heure précise et sélectionner les données correspondantes
                now = pd.Timestamp.now(tz='Europe/Berlin').floor('H')
                # Filtrer les données pour garder seulement celles de l'heure précédente
                hour_ago = now - pd.Timedelta(hours=1)
                past_hour_df = df[(df['time'] >= hour_ago) & (df['time'] < now)]
                # Filtrer les données pour garder seulement des 7 prochaines heures
                six_hours_later = now + pd.Timedelta(hours=7)
                next_six_hours_df = df[(df['time'] >= now) & (df['time'] < six_hours_later)]

                try:
                    # Vérifiez si le fichier existe pour déterminer si nous devons écrire l'en-tête
                    write_header = not os.path.exists(csv_filename)

                    # Enregistrer les données dans un CSV
                    past_hour_df.to_csv(csv_filename, mode='a', header=write_header, index=False)
                
                except Exception as e:
                    await log_message(f"Error in writing data to csv in get_weather_data: {str(e)}")           

                # Retourner les données pour les utiliser dans d'autres fonctions
                return next_six_hours_df
    
    except Exception as e:
        await log_message(f"Error in get_weather_data: {str(e)}")
        return pd.DataFrame()
    
async def send_alert(message):
    try:
        # Lire le fichier json pour obtenir tous les IDs de chat
        with open('chat_ids.json', 'r') as file:
            chats = json.load(file)
            
        for chat_id in chats:
            await bot.send_message(chat_id=chat_id, text=message)
        
        await log_message(f"Sent alert: {message}")
    except Exception as e:
        await log_message(f"Error in send_alert: {str(e)}")

async def check_weather():
    await log_message(f"Fonction check_weather executée")
    try:
        # Ajout du mot clé 'await' pour obtenir les données météo
        df = await get_weather_data()

        # Si le df est vide, on peut simplement retourner 
        if df.empty:
            await log_message(f"No data obtained from get_weather_data in check_weather.")
            return
    
        # Vérifier les conditions d'alerte
        for i, row in df.iterrows():
            # Convertir le temps unix en un objet datetime pour faciliter les comparaisons
            time = pd.to_datetime(row['time'], unit='s')
            
            # Vérifier les conditions de température
            if row['temperature'] > 35 or row['temperature'] < -10:
                # Vérifier si une alerte a déjà été envoyée pour ce jour
                if sent_alerts['temperature'] != time.date():
                    await send_alert(f"Alerte météo : Température prévue de {row['temperature']}°C à {time}.")
                    sent_alerts['temperature'] = time.date()
            
            # Vérifier les conditions de précipitations
            if row['precipitation_probability'] > 80 and row['precipitation'] > 5:
                if sent_alerts['precipitation'] != time.date():
                    await send_alert(f"Alerte météo : Précipitations prévues de {row['precipitation']}mm à {time}.")
                    sent_alerts['precipitation'] = time.date()

            # Vérifier les conditions de vent
            if row['windspeed'] > 30:
                if sent_alerts['windspeed'] != time.date():
                    await send_alert(f"Alerte météo : Vent prévu de {row['windspeed']}km/h à {time}.")
                    sent_alerts['windspeed'] = time.date()

            # Vérifier les conditions d'index UV
            if row['uv_index'] > 8:
                if sent_alerts['uv_index'] != time.date():
                    await send_alert(f"Alerte météo : Index UV prévu de {row['uv_index']} à {time}.")
                    sent_alerts['uv_index'] = time.date()

            # Vérifier les conditions de pression
            if len(df) >= 7:
                pressure_drop = df['pressure_msl'].iloc[0] - df['pressure_msl'].iloc[5]
                if pressure_drop >= 1.2:
                    if sent_alerts['pressure'] != time.date():
                        await send_alert(f"Alerte météo : Baisse rapide de la pression atmosphérique prévue de {pressure_drop} hPa sur 7 heures à partir de {time}.")
                        sent_alerts['pressure'] = time.date()
    
    except Exception as e:
        await log_message(f"Error in check_weather: {str(e)}")    

async def end_of_year_summary():
    try:
        # Lire toutes les données
        df = pd.read_csv(csv_filename)

        # si le df est vide, on peut simplement retourner 
        if df.empty:
            await log_message(f"Error in end_of_year_summary: No data found in the csv file.")
            return

        # Convertir 'time' en datetime
        df['time'] = pd.to_datetime(df['time'])

        # Filtrer pour la dernière année
        df = df[df['time'].dt.year == df['time'].dt.max().year]  # type: ignore


        # Trouver les valeurs max et min
        max_values = df.max()
        min_values = df.min()

        # Supprimer les valeurs indésirées
        max_values = max_values.drop(['precipitation_probability', 'precipitation'])
        min_values = min_values.drop(['precipitation_probability', 'precipitation', 'windspeed'])

        # Envoyer le résumé
        await send_alert(f"Résumé de fin d'année : Valeurs maximales - {max_values}. Valeurs minimales - {min_values}.")

    except Exception as e:
        await log_message(f"Error in end_of_year_summary: {str(e)}")

async def schedule_jobs():
    while True:
        try:
            await check_weather()
            now = datetime.datetime.now()
            if now.month == 12 and now.day == 31 and now.hour == 0:
                await end_of_year_summary()
            await asyncio.sleep(3600)
        except Exception as e:
            await log_message(f"Error in schedule_jobs: {str(e)}")

@dp.message_handler(commands='start')
async def start_command(message: types.Message):
    chat_id = message.chat.id
    
    # vérifier si le fichier json existe déjà
    if os.path.exists('chat_ids.json'):
        with open('chat_ids.json', 'r') as file:
            chats = json.load(file)
        # vérifier si l'ID du chat existe déjà
        if chat_id not in chats:
            chats.append(chat_id)
            with open('chat_ids.json', 'w') as file:
                json.dump(chats, file)
    else:
        # si le fichier n'existe pas, créer un nouveau fichier et ajouter l'ID du chat
        chats = [chat_id]
        with open('chat_ids.json', 'w') as file:
            json.dump(chats, file)
    
    await message.reply("Bot started!")

if __name__ == "__main__":
    from aiogram import executor
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(schedule_jobs())
    executor.start_polling(dp, skip_updates=True)