import re
import pandas as pd


def clean_city(city: str) -> str:
    if pd.isna(city):
        return city
    city = str(city).strip()
    city = re.sub(r'^(г\.|п\.|ст\.|д\.|к\.|С\.|клх)\s*', '', city, flags=re.IGNORECASE)
    return city.capitalize()


def transform_users_from_lake(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()

    if "name" in df.columns:
        df[['last_name', 'first_name', 'middle_name']] = df['name'].str.split(n=2, expand=True)
        df = df.drop(columns=['middle_name', 'name'])
    elif "fio" in df.columns:
        df[['last_name', 'first_name', 'middle_name']] = df['fio'].str.split(n=2, expand=True)
        df = df.drop(columns=['middle_name', 'fio'])

    if "city" in df.columns:
        df['city'] = df['city'].apply(clean_city)

    return df

def transform_resraurants_from_lake(df:pd.DataFrame) -> pd.DataFrame:
    if 'city' in df.columns:
        df['city'] = df['city'].apply(clean_city)
    return df