# Modulo para leer la fuente de datos

import pandas as pd
import os


def read_data(file_path):
    
    print(f"1. CARGA DE DATOS")
    print(f"Leyendo datos desde: {file_path}")
    try:
        df = pd.read_csv(file_path)
        print(f"Se han leído {df.shape[0]} registros con {df.shape[1]} campos.")
        return df
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return None

# Carga los datos desde el csv y devuelve un dataframe de pandas
def load_csv_data(file_path):
    try:
        if not os.path.exists(file_path):
            print(f"¡Error! El archivo no existe: {file_path}")
            return None

        df = pd.read_csv(file_path)
        print(f"Datos completos cargados desde: {file_path}")
            
        print(f"Dimensiones del dataset: {df.shape[0]} filas x {df.shape[1]} columnas")
        return df
        
    except Exception as e:
        print(f"Error al cargar el archivo CSV: {e}")
        return None