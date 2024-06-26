'''
*
* PROJET : MeteoCheck
* AUTEUR : Arnaud R.
* VERSIONS : 1.3.7
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
import os

script_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_dir)
config_path = os.path.join(script_dir, 'config.ini')

config = configparser.ConfigParser()
config.read(config_path)

TOKEN_TELEGRAM = config['KEYS']['TELEGRAM_BOT_TOKEN']
# Variable globale pour la ville
VILLE = "Versoix"
# Créer le bot Telegram
bot = Bot(token=TOKEN_TELEGRAM)
# Ajouté un chat_id
dp = Dispatcher(bot, storage=MemoryStorage())

# Définir l'URL de l'API météo
weather_url = "https://api.open-meteo.com/v1/forecast?latitude=46.2838&longitude=6.1621&hourly=temperature_2m,precipitation_probability,precipitation,pressure_msl,windspeed_10m,uv_index,relativehumidity_2m&timezone=GMT&forecast_days=2&past_days=2&models=best_match&timeformat=unixtime"

# Définir le nom du fichier CSV où stocker les données
csv_filename = "weather_data.csv"

# Vérifiez si le fichier existe, sinon créez-le.
if not os.path.exists(csv_filename):
    # Créer un DataFrame vide avec les bonnes colonnes
    df = pd.DataFrame(columns=['time', 'temperature', 'precipitation_probability', 'precipitation', 'windspeed', 'uv_index', 'pressure_msl', 'humidity'])
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

async def get_weather_data():
    try:
        # Arrondir l'heure actuelle à l'heure précise dès le début
        now = pd.Timestamp.now(tz='Europe/Berlin').floor('h')

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
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
                humidity = data['hourly']['relativehumidity_2m']

                # Créer un DataFrame avec les données
                df = pd.DataFrame({
                    'time': time,
                    'temperature': temperature,
                    'precipitation_probability': precipitation_probability,
                    'precipitation': precipitation,
                    'windspeed': windspeed,
                    'uv_index': uv_index,
                    'pressure_msl': pressure_msl, 
                    'humidity': humidity  
                })

                # Convertir le temps unix en un objet datetime pour faciliter les comparaisons
                df['time'] = pd.to_datetime(df['time'], unit='s', utc=True).dt.tz_convert('Europe/Berlin').dt.floor('h')

                # Lire le fichier csv existant
                df_existing = pd.read_csv(csv_filename)
                df_existing['time'] = pd.to_datetime(df_existing['time'], utc=True)

                # Créez un DataFrame pour les 24 dernières heures sans l'heure actuelle d'où le < now et non pas <= !
                twenty_four_hours_ago = now - pd.Timedelta(hours=24)
                last_twenty_four_hours_df = df[(df['time'] >= twenty_four_hours_ago) & (df['time'] < now)]

                # Vérifiez si le fichier existe pour déterminer si nous devons écrire l'en-tête
                write_header = not os.path.exists(csv_filename)

                # Vérifiez si ces données existent déjà dans le fichier CSV
                missing_data = last_twenty_four_hours_df[~last_twenty_four_hours_df['time'].isin(df_existing['time'])]

                # S'il manque des données, ajoutez-les au fichier CSV
                if not missing_data.empty:
                    missing_data.to_csv(csv_filename, mode='a', header=write_header, index=False)
                    await log_message(f"Enregistrement des données manquantes dans le CSV")

                # Filtrer les données pour garder seulement des 7 prochaines heures
                seven_hours_later = now + pd.Timedelta(hours=7)
                next_seven_hours_df = df[(df['time'] > now) & (df['time'] <= seven_hours_later)]

                # Filtrer les données pour garder seulement les 24 prochaines heures
                twenty_four_hours_later = now + pd.Timedelta(hours=24)
                next_twenty_four_hours_df = df[(df['time'] > now) & (df['time'] <= twenty_four_hours_later)]

                # Retourner les données pour les utiliser dans d'autres fonctions
                return next_seven_hours_df, next_twenty_four_hours_df

    except Exception as e:
        await log_message(f"Error in get_weather_data: {str(e)}")
        return pd.DataFrame()
    
async def check_weather():
    await log_message(f"Fonction check_weather executée")
    try:
        # Ajout du mot clé 'await' pour obtenir les données météo
        df_next_seven_hours, df_next_twenty_four_hours = await get_weather_data()

        # Si après 2 tentatives, aucune donnée n'est obtenue, log l'erreur et return
        if df_next_seven_hours.empty and df_next_twenty_four_hours.empty: # type: ignore
            await log_message(f"Aucune donnée obtenue à partir de get_weather_data dans check_weather! Nouvelle vérification la prochaine heure.")
            return

        # Vérifier les conditions d'alerte pour les 7 prochaines heures
        for i, row in df_next_seven_hours.iterrows(): # type: ignore
            # Convertir le temps unix en un objet datetime pour faciliter les comparaisons
            time = pd.to_datetime(row['time'], unit='s')
            
            # Vérifier les conditions de température
            if row['temperature'] > 35 or row['temperature'] < -10:
                # Vérifier si une alerte a déjà été envoyée pour ce jour
                if sent_alerts['temperature'] != time.date():
                    await send_alert(f"Alerte météo : Température prévue de {row['temperature']}°C à {time} à Versoix.", row, 'temperature')
                    sent_alerts['temperature'] = time.date()
            
            # Vérifier les conditions de précipitations
            if row['precipitation_probability'] > 80 and row['precipitation'] > 15 :
                if sent_alerts['precipitation'] != time.date():
                    await send_alert(f"Alerte météo : Fortes pluies prévues de {row['precipitation']}mm à {time} à Versoix.", row, 'precipitation')
                    sent_alerts['precipitation'] = time.date()

            # Vérifier les conditions de vent
            if row['windspeed'] > 60 and row['windspeed'] <= 75:
                if sent_alerts['windspeed'] != time.date():
                    await send_alert(f"Alerte météo : Vent fort prévu de {row['windspeed']}km/h à {time} à Versoix.", row, 'windspeed')
                    sent_alerts['windspeed'] = time.date()

            if row['windspeed'] > 75:
                if sent_alerts['windspeed'] != time.date():
                    await send_alert(f"Alerte météo : Vent tempétueux prévu de {row['windspeed']}km/h à {time} à Versoix.", row, 'windspeed')
                    sent_alerts['windspeed'] = time.date()

            # Vérifier les conditions d'index UV
            if row['uv_index'] > 8:
                if sent_alerts['uv_index'] != time.date():
                    await send_alert(f"Alerte météo : Index UV prévu de {row['uv_index']} à {time} à Versoix.", row, 'uv_index')
                    sent_alerts['uv_index'] = time.date()

        # Vérifier les conditions d'alerte pour les 24 prochaines heures
        for i, row in df_next_twenty_four_hours.iterrows(): # type: ignore
            # Convertir le temps unix en un objet datetime pour faciliter les comparaisons
            time = pd.to_datetime(row['time'], unit='s')
            
            # Vérifier les conditions de pression
            if len(df_next_twenty_four_hours) >= 24: # type: ignore
                pressure_drop = df_next_twenty_four_hours['pressure_msl'].iloc[0] - df_next_twenty_four_hours['pressure_msl'].iloc[23] # type: ignore
                if pressure_drop >= 20:
                    if sent_alerts['pressure_msl'] != time.date():
                        await send_alert(f"Alerte météo : Risque de bombe météorologique détecté. Baisse de pression prévue de {round(pressure_drop, 2)} hPa sur 24 heures à partir de {time}.", row, 'pressure_msl')
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
        await send_alert(f"Alerte météo : Nouveau record annuel possible de {alert_column} à {row[alert_column]} à {row['time']}.")

    # Vérifier si la valeur est inférieure au minimum pour la colonne "température"
    if alert_column == 'temperature':
        min_value = df[alert_column].min()
        if row[alert_column] < min_value:
            await send_alert(f"Alerte météo : Nouveau record minimum annuel possible de {alert_column} à {row[alert_column]} à {row['time']}.")

async def end_of_month_summary():
    try:
        df = pd.read_csv(csv_filename)

        # Supprimer les éventuelles valeurs manquantes dans la colonne 'time'
        df = df.dropna(subset=['time'])

        # Convertir la colonne 'time' en type datetime
        df['time'] = pd.to_datetime(df['time'])

        # Vérifier le type de la colonne 'time'
        await log_message(f"Time column dtype: {df['time'].dtype}")

        # Récupérer le mois en cours
        current_month = df['time'].dt.to_period('M').max()

        # Filtrer le DataFrame pour garder seulement les données du mois en cours
        df = df[df['time'].dt.to_period('M') == current_month]

        # Grouper les données par jour et calculer la somme des précipitations pour chaque jour
        df['day'] = df['time'].dt.date
        df['daily_precipitation'] = df.groupby('day')['precipitation'].transform('sum')

        # Définir l'index du DataFrame comme étant la colonne 'day'
        df.set_index('day', inplace=True)

        # Trouver le jour le plus chaud
        hot_day = df['temperature'].idxmax()
        max_temp = df.loc[hot_day, 'temperature'].max()

        # Trouver le jour le plus froid
        cold_day = df['temperature'].idxmin()
        min_temp = df.loc[cold_day, 'temperature'].min()

        # Trouver le jour avec le plus de pluie
        rain_day = df['daily_precipitation'].idxmax()
        max_precipitation = df.loc[rain_day, 'daily_precipitation'].max()

        # Trouver le jour avec le plus fort index UV
        uv_day = df['uv_index'].idxmax()
        max_uv_index = df.loc[uv_day, 'uv_index'].max()

        # Compter le nombre de jours de pluie
        rain_threshold = 0.1
        rainy_days = df[df['daily_precipitation'] > rain_threshold].index.nunique()

        # Envoyer le résumé de fin de mois
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
            # Vérification pour la fin de mois avec une tolérance d'une minute
            if now.day == 1 and now.hour == 0 and now.minute < 1:
                await end_of_month_summary()
            # Vérification pour la fin d'année avec une tolérance d'une minute
            elif now.month == 12 and now.day == 31 and now.hour == 0 and now.minute < 1:
                await end_of_year_summary()
            await asyncio.sleep(60)  # Exécution toutes les minutes
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

@dp.message_handler(commands='weather')
async def get_latest_info_command(message: types.Message):
    try:
        # Lire le CSV et obtenir la dernière ligne
        df = pd.read_csv(csv_filename)
        if df.empty:
            await message.reply("Aucune donnée disponible.")
        else:
            latest_info = df.iloc[-1].to_dict()
            latest_info['time'] = pd.to_datetime(latest_info['time']).strftime("%Y-%m-%d %H:%M:%S")
            response = f"Dernières informations météo pour {VILLE} :\n"
            for key, value in latest_info.items():
                # Ajout des unités spécifiques pour chaque mesure
                if key == 'temperature':
                    response += f"Température: {value}°C\n"
                elif key == 'precipitation_probability':
                    response += f"Probabilité de précipitation: {value}%\n"
                elif key == 'precipitation':
                    response += f"Précipitation: {value}mm\n"
                elif key == 'windspeed':
                    response += f"Vitesse du vent: {value}km/h\n"
                elif key == 'uv_index':
                    response += f"Indice UV: {value}\n"
                elif key == 'pressure_msl':
                    response += f"Pression au niveau de la mer: {value} hPa\n"
                elif key == 'humidity':
                    response += f"Humidité: {value}%\n"
                else:
                    response += f"{key}: {value}\n"
            await message.reply(response)
    except Exception as e:
        await message.reply(f"Erreur lors de l'obtention des informations : {str(e)}")
        await log_message(f"Error in get_latest_info_command: {str(e)}")

dp.register_message_handler(get_latest_info_command, commands='weather')

if __name__ == "__main__":
    from aiogram import executor
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(schedule_jobs())
    executor.start_polling(dp, skip_updates=True)