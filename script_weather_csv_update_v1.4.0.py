import pandas as pd
import pytz
import os

csv_filename = 'weather_data.csv'
new_csv_filename = 'weather_data_updated.csv'

# Lire le CSV existant
df = pd.read_csv(csv_filename)

# Fonction pour convertir les dates en UTC
def convert_to_utc(date_string):
    try:
        dt = pd.to_datetime(date_string, format='%Y-%m-%d %H:%M:%S%z')
    except ValueError:
        try:
            dt = pd.to_datetime(date_string, format='%Y-%m-%d %H:%M:%S')
            berlin_tz = pytz.timezone('Europe/Berlin')
            dt = berlin_tz.localize(dt)
        except ValueError:
            return pd.NaT
    return dt.astimezone(pytz.UTC)

# Appliquer la conversion à la colonne 'time'
df['time'] = df['time'].apply(convert_to_utc)

# Mettre à jour les noms des colonnes
column_mapping = {
    'temperature': 'temperature_2m',
    'windspeed': 'windspeed_10m',
    'humidity': 'relativehumidity_2m'
}

df = df.rename(columns=column_mapping)

# Vérifier s'il y a des valeurs NaT après la conversion
if df['time'].isna().any():
    print("Attention : Certaines valeurs de date n'ont pas pu être converties. Veuillez vérifier votre fichier CSV.")
    print(df[df['time'].isna()])
else:
    # Sauvegarder le CSV mis à jour avec les dates en UTC et les nouveaux noms de colonnes
    df.to_csv(new_csv_filename, index=False, date_format='%Y-%m-%dT%H:%M:%SZ')
    print(f"Conversion terminée. Les données sont maintenant en UTC avec les colonnes mises à jour dans '{new_csv_filename}'.")

    # Remplacer l'ancien fichier par le nouveau
    os.remove(csv_filename)
    os.rename(new_csv_filename, csv_filename)
    print(f"Le fichier original '{csv_filename}' a été mis à jour.")

# Vérifier si toutes les colonnes nécessaires sont présentes
required_columns = ['time', 'temperature_2m', 'precipitation_probability', 'precipitation', 'windspeed_10m', 'uv_index', 'pressure_msl', 'relativehumidity_2m']
missing_columns = set(required_columns) - set(df.columns)

if missing_columns:
    print(f"Attention : Les colonnes suivantes sont manquantes : {', '.join(missing_columns)}")
    # Ajouter les colonnes manquantes avec des valeurs NaN
    for col in missing_columns:
        df[col] = pd.NA
    
    # Sauvegarder le CSV mis à jour avec les nouvelles colonnes
    df.to_csv(csv_filename, index=False, date_format='%Y-%m-%dT%H:%M:%SZ')
    print(f"Les colonnes manquantes ont été ajoutées à '{csv_filename}'.")

print("\nColonnes finales du DataFrame:")
print(df.columns)

print("\nTypes de données des colonnes:")
print(df.dtypes)

print("\nAperçu des données (5 premières lignes):")
print(df.head())

print("\nRésumé statistique des données numériques:")
print(df.describe())

print("\nNombre total d'enregistrements:")
print(len(df))

print("\nValeurs uniques dans chaque colonne:")
for column in df.columns:
    print(f"\n{column}:")
    print(df[column].value_counts(dropna=False).head())

print("\nVérification des valeurs manquantes:")
print(df.isnull().sum())

print("\nPlage de dates dans le dataset:")
print(f"De: {df['time'].min()}")
print(f"À: {df['time'].max()}")