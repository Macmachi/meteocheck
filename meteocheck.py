'''
*
* PROJET : MeteoCheck
* AUTEUR : Arnaud R.
* VERSIONS : 1.1.3
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
from aiogram import exceptions
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

def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    error_message = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    asyncio.run(log_message(f"Uncaught exception: {error_message}"))

sys.excepthook = log_uncaught_exceptions

async def log_message(message: str):
    async with aiofiles.open("log_meteocheck.log", mode='a') as f:
        await f.write(f"{datetime.datetime.now()} - {message}\n")

# Définir les alertes déjà envoyées pour le jour en cours
sent_alerts = {
    'temperature': None,
    'precipitation': None,
    'windspeed': None,
    'uv_index': None,
    'pressure_msl': None
}

async def get_weather_data():
    try:
        # Arrondir l'heure actuelle à l'heure précise dès le début
        now = pd.Timestamp.now(tz='Europe/Berlin').floor('H')

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

                # Lire le fichier csv existant
                df_existing = pd.read_csv(csv_filename)
                df_existing['time'] = pd.to_datetime(df_existing['time'])

                # Filtrer les données pour garder seulement celles de deux heures en arrière
                two_hours_ago = now - pd.Timedelta(hours=2)
                past_two_hours_df = df[(df['time'] >= two_hours_ago) & (df['time'] < now)]

                # Vérifier si les deux heures en arrière ont déjà été enregistrées
                past_two_hours_exists_in_csv = not df_existing[(df_existing['time'] >= two_hours_ago) & (df_existing['time'] < now)].empty
                if not past_two_hours_exists_in_csv:
                    # Si les deux heures en arrière n'ont pas encore été enregistrées, ajouter seulement les données de ces deux heures
                    try:
                        # Vérifiez si le fichier existe pour déterminer si nous devons écrire l'en-tête
                        write_header = not os.path.exists(csv_filename)

                        # Enregistrer les données dans un CSV
                        past_two_hours_df.to_csv(csv_filename, mode='a', header=write_header, index=False)
                        await log_message(f"Enregistrement des données dans le CSV")
                
                    except Exception as e:
                        await log_message(f"Error in writing data to csv in get_weather_data: {str(e)}")

                # Filtrer les données pour garder seulement celles de l'heure précédente
                hour_ago = now - pd.Timedelta(hours=1)
                past_hour_df = df[(df['time'] >= hour_ago) & (df['time'] < now)]
                
                # Vérifier si l'heure précédente a déjà été enregistrée
                past_hour_exists_in_csv = not df_existing[df_existing['time'] == hour_ago].empty
                if not past_hour_exists_in_csv:
                    # Si l'heure précédente n'a pas encore été enregistrée, ajouter seulement les données de l'heure précédente
                    try:
                        # Vérifiez si le fichier existe pour déterminer si nous devons écrire l'en-tête
                        write_header = not os.path.exists(csv_filename)

                        # Enregistrer les données dans un CSV
                        past_hour_df.to_csv(csv_filename, mode='a', header=write_header, index=False)
                        await log_message(f"Enregistrement des données dans le CSV")
                
                    except Exception as e:
                        await log_message(f"Error in writing data to csv in get_weather_data: {str(e)}")

                # Filtrer les données pour garder seulement des 7 prochaines heures
                seven_hours_later = now + pd.Timedelta(hours=7)
                next_seven_hours_df = df[(df['time'] > now) & (df['time'] <= seven_hours_later)]

                # Retourner les données pour les utiliser dans d'autres fonctions
                return next_seven_hours_df

    except Exception as e:
        await log_message(f"Error in get_weather_data: {str(e)}")
        return pd.DataFrame()
    
async def send_alert(message, row=None, alert_column=None):
    if row is not None and alert_column is not None:
        await check_records(row, alert_column)

    try:
        # Lire le fichier json pour obtenir tous les IDs de chat
        with open('chat_ids.json', 'r') as file:
            chats = json.load(file)

        for chat_id in chats:
            success = False
            while not success:
                try:
                    await bot.send_message(chat_id=chat_id, text=message)
                    success = True  # Si l'envoi du message est réussi, marquer comme réussi et sortir de la boucle while
                except exceptions.TelegramAPIError as e:
                    print(f"Erreur rencontrée lors de l'envoi du message au chat {chat_id} : {e}")
                    await asyncio.sleep(1)  # attendre une seconde avant de réessayer
                
        await log_message(f"Sent alert: {message}")
    except Exception as e:
        await log_message(f"Error in send_alert: {str(e)}")

async def check_weather():
    await log_message(f"Fonction check_weather executée")
    try:
        # Ajout du mot clé 'await' pour obtenir les données météo
        df = await get_weather_data()

        # Si le df est vide, on peut simplement retourner 
        retry_count = 0  # Compteur pour le nombre de tentatives
        while df.empty and retry_count < 2:  # Limite le nombre de tentatives à 2
            await log_message(f"No data obtained from get_weather_data in check_weather. Retry count: {retry_count}")
            await asyncio.sleep(300)  # Attendez 300 secondes avant de réessayer
            df = await get_weather_data()
            retry_count += 1

        # Si après 5 tentatives, aucune donnée n'est obtenue, log l'erreur et return
        if df.empty:
            await log_message(f"No data obtained from get_weather_data in check_weather after 2 attempts.")
            return
    
        # Vérifier les conditions d'alerte
        for i, row in df.iterrows():
            # Convertir le temps unix en un objet datetime pour faciliter les comparaisons
            time = pd.to_datetime(row['time'], unit='s')
            
            # Vérifier les conditions de température
            if row['temperature'] > 35 or row['temperature'] < -10:
                # Vérifier si une alerte a déjà été envoyée pour ce jour
                if sent_alerts['temperature'] != time.date():
                    await send_alert(f"Alerte météo : Température prévue de {row['temperature']}°C à {time}.", row, 'temperature')
                    sent_alerts['temperature'] = time.date()
            
            # Vérifier les conditions de précipitations
            if row['precipitation_probability'] > 80 and row['precipitation'] > 5:
                if sent_alerts['precipitation'] != time.date():
                    await send_alert(f"Alerte météo : Précipitations prévues de {row['precipitation']}mm à {time}.", row, 'precipitation')
                    sent_alerts['precipitation'] = time.date()

            # Vérifier les conditions de vent
            if row['windspeed'] > 30:
                if sent_alerts['windspeed'] != time.date():
                    await send_alert(f"Alerte météo : Vent prévu de {row['windspeed']}km/h à {time}.", row, 'windspeed')
                    sent_alerts['windspeed'] = time.date()

            # Vérifier les conditions d'index UV
            if row['uv_index'] > 8:
                if sent_alerts['uv_index'] != time.date():
                    await send_alert(f"Alerte météo : Index UV prévu de {row['uv_index']} à {time}.", row, 'uv_index')
                    sent_alerts['uv_index'] = time.date()

            # Vérifier les conditions de pression
            if len(df) >= 6:
                pressure_drop = df['pressure_msl'].iloc[0] - df['pressure_msl'].iloc[5]
                if pressure_drop >= 1.2:
                    if sent_alerts['pressure_msl'] != time.date():
                        await send_alert(f"Alerte météo : Baisse rapide de la pression atmosphérique prévue de {pressure_drop} hPa sur 6 heures à partir de {time}.", row, 'pressure_msl')
                        sent_alerts['pressure_msl'] = time.date()
    
    except Exception as e:
        await log_message(f"Error in check_weather: {str(e)}")    

async def check_records(row, alert_column):
    # Lire toutes les données
    df = pd.read_csv(csv_filename)

    # si le df est vide, on peut simplement retourner 
    if df.empty:
        await log_message(f"No data found in the csv file.")
        return

    # Convertir 'time' en datetime
    df['time'] = pd.to_datetime(df['time'])

    # Obtenir l'année de la ligne actuelle
    current_year = row['time'].year

    # Filtrer le dataframe pour n'inclure que l'année actuelle
    df = df[df['time'].dt.year == current_year]

    # Vérifier le record pour la colonne d'alerte
    max_value = df[alert_column].max()
    if row[alert_column] > max_value:
        await send_alert(f"Alerte météo : Nouveau record de {alert_column} à {row[alert_column]} à {row['time']}.")

async def end_of_month_summary():
    try:
        df = pd.read_csv(csv_filename)
        df['time'] = pd.to_datetime(df['time'])

        current_month = df['time'].dt.to_period('M').max()
        df = df[df['time'].dt.to_period('M') == current_month]

        # Group by day and sum precipitation for each day
        df['day'] = df['time'].dt.date
        df['daily_precipitation'] = df.groupby('day')['precipitation'].transform('sum')

        df.set_index('day', inplace=True)

        hot_day = df['temperature'].idxmax()
        max_temp = df.loc[hot_day, 'temperature'].max() # type: ignore

        cold_day = df['temperature'].idxmin()
        min_temp = df.loc[cold_day, 'temperature'].min() # type: ignore

        rain_day = df['daily_precipitation'].idxmax()
        max_precipitation = df.loc[rain_day, 'daily_precipitation'].max() # type: ignore

        uv_day = df['uv_index'].idxmax()
        max_uv_index = df.loc[uv_day, 'uv_index'].max() # type: ignore

        rain_threshold = 0.1
        rainy_days = df[df['daily_precipitation'] > rain_threshold].index.nunique()

        await send_alert(f"Résumé de fin de mois : Le jour le plus chaud était {hot_day} avec {max_temp}°C. Le jour le plus froid était {cold_day} avec {min_temp}°C. Le jour avec le plus de pluie était {rain_day} avec {max_precipitation}mm. Le jour avec le plus fort index UV était {uv_day} avec un index de {max_uv_index}. Le nombre de jours de pluie était {rainy_days}.")

    except Exception as e:
        await log_message(f"Error in end_of_month_summary: {str(e)}")

async def end_of_year_summary():
    try:
        df = pd.read_csv(csv_filename)
        df['time'] = pd.to_datetime(df['time'])

        current_year = df['time'].dt.to_period('Y').max()
        df = df[df['time'].dt.to_period('Y') == current_year]

        # Group by day and sum precipitation for each day
        df['day'] = df['time'].dt.date
        df['daily_precipitation'] = df.groupby('day')['precipitation'].transform('sum')

        df.set_index('day', inplace=True)

        hot_day = df['temperature'].idxmax()
        max_temp = df.loc[hot_day, 'temperature'].max() # type: ignore

        cold_day = df['temperature'].idxmin()
        min_temp = df.loc[cold_day, 'temperature'].min() # type: ignore

        rain_day = df['daily_precipitation'].idxmax()
        max_precipitation = df.loc[rain_day, 'daily_precipitation'].max() # type: ignore

        uv_day = df['uv_index'].idxmax()
        max_uv_index = df.loc[uv_day, 'uv_index'].max() # type: ignore

        wind_day = df['wind_speed'].idxmax()
        max_wind_speed = df.loc[wind_day, 'wind_speed'].max() # type: ignore

        rain_threshold = 0.1
        rainy_days = df[df['daily_precipitation'] > rain_threshold].index.nunique()

        await send_alert(f"Résumé de fin d'année : Le jour le plus chaud était {hot_day} avec {max_temp}°C. Le jour le plus froid était {cold_day} avec {min_temp}°C. Le jour avec le plus de pluie était {rain_day} avec {max_precipitation}mm. Le jour avec le plus fort index UV était {uv_day} avec un index de {max_uv_index}. Le jour avec le vent le plus fort était {wind_day} avec une vitesse de {max_wind_speed} km/h. Le nombre de jours de pluie était {rainy_days}.")

    except Exception as e:
        await log_message(f"Error in end_of_year_summary: {str(e)}")

async def schedule_jobs():
    while True:
        try:
            await check_weather()
            now = datetime.datetime.now()
            if now.day == 1 and now.hour == 0:
                await end_of_month_summary()
            elif now.month == 12 and now.day == 31 and now.hour == 0:
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