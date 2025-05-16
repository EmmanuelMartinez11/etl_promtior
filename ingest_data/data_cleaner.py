import pandas as pd
import re
import os
from datetime import datetime

def clean_data(df: pd.DataFrame, output_path: str = 'data/cleaned_data.parquet') -> pd.DataFrame:
    df_clean = df.copy()

    # 1. Estandarizar columnas y clasificarlas
    std_columns, column_types = standardize_column_names(df_clean.columns)
    df_clean.columns = std_columns
    
    # 2. Normalizar texto
    for col in df_clean.select_dtypes(include=['object']):
        df_clean[col] = (
            df_clean[col]
            .astype(str)
            .str.strip()
            .replace({'nan': None})
        )

    # 3. Conversión de tipos según la clasificación generada
    df_clean = convert_columns(df_clean, column_types)

    # 4. Extracción de coordenadas
    df_clean = extract_coordinates(df_clean)
    
    # 5. Convertir código de estado "WA" a "Washington"
    if 'state' in df_clean.columns:
        df_clean['state'] = df_clean['state'].replace({'WA': 'Washington'})

    # 6. Generar date_key a partir de model_year
    if 'model_year' in df_clean.columns:
        df_clean['model_year'] = pd.to_numeric(df_clean['model_year'], errors='coerce')
        df_clean['registration_date'] = pd.to_datetime(
            df_clean['model_year'], format='%Y', errors='coerce'
        )
        df_clean['date_key'] = df_clean['registration_date'].dt.strftime('%Y%m%d').astype(float).astype('Int64')

    # 7. Manejo de faltantes según el tipo de columna
    df_clean = fill_missing(df_clean, column_types, num_fill=-1) 

    # 8. Guardar parquet
    save_to_parquet(df_clean, output_path)
    return df_clean

# Estandariza el nombre de las columnas en snake_case y las clasifica por tipo. 
# Esta clasificacion es para asignar los tipos de datos que yo quiera con tal de mejorar el rendimiento, como por ejemplo, pasar el year a numero
def standardize_column_names(cols):
    new_cols = []
    column_types = {}
    
    numeric_columns = [
        'model_year', 'electric_range', 'base_msrp', 'dol_vehicle_id', 
        'legislative_district', 'census_tract'
    ]
    
    date_columns = [
        'registration_date'
    ]
    
    point_columns = [
        'vehicle_location'
    ]
    
    for col in cols:
        # Estandariza nombre
        name = col.lower()
        name = re.sub(r'[^a-z0-9]+', '_', name)
        name = re.sub(r'_+', '_', name).strip('_')
        new_cols.append(name)
        
        # Clasifica por tipo
        if name in numeric_columns or any(name.endswith(f'_{suffix}') for suffix in ['id', 'year', 'range', 'msrp', 'price', 'cost']):
            column_types[name] = 'numeric'
        elif name in date_columns or any(pattern in name for pattern in ['date', 'time']):
            column_types[name] = 'date'
        elif name in point_columns or 'location' in name:
            column_types[name] = 'point'
        else:
            column_types[name] = 'text'
            
    return new_cols, column_types

# Convierte las columnas según la clasificación de standardize_column_names
def convert_columns(df: pd.DataFrame, column_types: dict) -> pd.DataFrame:
    for col in df.columns:
        if col in column_types:
            if column_types[col] == 'numeric':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif column_types[col] == 'date':
                df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

# Busca en texto columnas con 'POINT (...)' y extrae lat/lon.
def extract_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == object and df[col].astype(str).str.contains('POINT', na=False).any():
            coords = df[col].dropna().astype(str).str.extract(r'POINT \((?P<lon>[-\d\.]+)\s+(?P<lat>[-\d\.]+)\)')
            if not coords.empty and 'lon' in coords and 'lat' in coords:
                df['longitude'] = pd.to_numeric(coords['lon'], errors='coerce')
                df['latitude'] = pd.to_numeric(coords['lat'], errors='coerce')
                break
    return df

# Rellena valores faltantes según el tipo de columna
def fill_missing(df: pd.DataFrame, column_types: dict, num_fill: float = 0, text_fill: str = 'Unknown') -> pd.DataFrame:
    
    # Columnas numéricas
    numeric_cols = [col for col in df.columns if column_types.get(col) == 'numeric' 
                   or df[col].dtype.name in ['int64', 'float64', 'Int64', 'Float64']]
    for col in numeric_cols:
        df[col] = df[col].fillna(num_fill)

    # Columnas de texto 
    text_cols = [col for col in df.columns if column_types.get(col) == 'text' 
                or (df[col].dtype == object and col not in numeric_cols)]
    for col in text_cols:
        df[col] = df[col].fillna(text_fill)

    return df

# Funcion para guardar un dataframe limpio (esto facilita la orquestacion)
def save_to_parquet(df: pd.DataFrame, output_path: str = 'data/cleaned_data.parquet') -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine='pyarrow', index=False)
    print(f"Guardado: {output_path} ({df.shape[0]} filas, {df.shape[1]} columnas)")