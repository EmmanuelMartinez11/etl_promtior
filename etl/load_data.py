import pandas as pd
import os
import time
import re
from datetime import datetime, timedelta
from psycopg2.extras import execute_batch
from etl.db_connection import get_connection

# Funcion que busca columnas en el DataFrame que coincidan con el patrón especificado, ignorando mayúsculas/minúsculas y caracteres especiales.
# Retorna el nombre de la primera columna que coincida
def find_column(pattern: str, df_columns: list) -> str:
    pattern_normalized = re.sub(r'[^a-z0-9]', '', pattern.lower())
    
    for col in df_columns:
        col_normalized = re.sub(r'[^a-z0-9]', '', col.lower())
        if pattern_normalized in col_normalized:
            return col
    return None

# Carga la tabla dimensional dim_vehicle con datos de vehículos.
# Aplica SCD Tipo 1: actualiza registros existentes basado en VIN.
# Elimina duplicados
def load_dim_vehicle(data_file_path: str) -> bool:

    print("\nCargando dimensión Vehicle...")
    
    try:
        df = pd.read_parquet(data_file_path)
        
        # Mapeo de columnas requeridas
        column_mapping = {
            'vin': find_column('vin', df.columns),
            'model_year': find_column('model_year', df.columns) or find_column('year', df.columns),
            'make': find_column('make', df.columns),
            'model': find_column('model', df.columns)
        }
        
        # Verificar columnas faltantes
        missing_columns = [col for col, found in column_mapping.items() if found is None]
        if missing_columns:
            print(f"Columnas faltantes en Vehicle: {missing_columns}")
            

        # Preparar datos
        df_vehicle = df[list(column_mapping.values())].rename(columns={v: k for k, v in column_mapping.items()})
        df_vehicle = df_vehicle.drop_duplicates('vin').dropna(subset=['vin'])
        df_vehicle['vin'] = df_vehicle['vin'].astype(str).str.strip()

        # Insertar en base de datos
        conn = get_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO dim_vehicle (vin, model_year, make, model)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (vin) DO UPDATE SET
                model_year = EXCLUDED.model_year,
                make = EXCLUDED.make,
                model = EXCLUDED.model
        """
        
        data_tuples = [tuple(row) for row in df_vehicle.to_numpy()]
        execute_batch(cursor, insert_query, data_tuples, page_size=1000)
        conn.commit()
        
        print(f"Vehículos cargados: {len(df_vehicle)}")
        
    except Exception as e:
        conn.rollback()
        print(f"Error cargando dimensión Vehicle: {e}")
    finally:
        cursor.close()
        conn.close()


    
# Carga la tabla dimensional dim_date con todas las fechas dentro del rango de años de los vehículos. Genera atributos adicionales como trimestre, mes, etc.
def load_dim_date(data_file_path: str) -> bool:
    print("\nCargando dimensión Date...")
    
    try:
        df = pd.read_parquet(data_file_path)
        
        # Obtener rango de años válido
        min_year = max(df['model_year'].min(), 1900)  # Año mínimo razonable
        max_year = min(df['model_year'].max(), datetime.now().year)  # Evitar años futuros
        
        start_date = datetime(int(min_year), 1, 1)
        end_date = datetime(int(max_year), 12, 31)
        
        conn = get_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO dim_date (
                id_date, full_date, year, quarter, month, month_name, 
                day, day_of_week, is_weekend
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_date) DO NOTHING
        """
        
        current_date = start_date
        records_count = 0
        
        while current_date <= end_date:
            date_key = int(current_date.strftime('%Y%m%d'))
            quarter = f'Q{(current_date.month - 1)//3 + 1}'
            is_weekend = current_date.weekday() >= 5
            
            date_record = (
                date_key, current_date.date(), current_date.year, quarter,
                current_date.month, current_date.strftime('%B'), current_date.day,
                current_date.strftime('%A'), is_weekend
            )
            
            cursor.execute(insert_query, date_record)
            records_count += 1
            current_date += timedelta(days=1)
        
        conn.commit()
        print(f"Fechas cargadas: {records_count}")
        
        
    except Exception as e:
        conn.rollback()
        print(f"Error cargando dimensión Date: {e}")
        
    finally:
        cursor.close()
        conn.close()

# Carga la tabla dimensional dim_location con datos geográficos.
# Aplica SCD Tipo 1: actualiza registros con la misma combinación condado+ciudad+estado+código postal.
def load_dim_location(data_file_path: str) -> bool:
    print("\nCargando dimensión Location...")
    
    try:
        df = pd.read_parquet(data_file_path)
        
        # Mapeo de columnas
        column_mapping = {
            'county': find_column('county', df.columns),
            'city': find_column('city', df.columns),
            'state': find_column('state', df.columns),
            'postal_code': find_column('postal_code', df.columns) or find_column('postal', df.columns) or find_column('zip', df.columns),
            'census_tract': find_column('census_tract', df.columns) or find_column('census', df.columns),
            'electric_utility': find_column('electric_utility', df.columns) or find_column('utility', df.columns),
            'latitude': find_column('latitude', df.columns),
            'longitude': find_column('longitude', df.columns)
        }
        
        # Verificar columnas esenciales
        essential_columns = ['county', 'city', 'state']
        missing_essential = [col for col in essential_columns if column_mapping[col] is None]
        if missing_essential:
            print(f"Faltan columnas esenciales en Location: {missing_essential}")
            
        
        # Preparar datos
        df_location = df[list(column_mapping.values())].rename(columns={v: k for k, v in column_mapping.items()})
        df_location = df_location.fillna({'state': 'WA'}).drop_duplicates()
        df_location['census_tract'] = df_location['census_tract'].astype(str).str.slice(0, 25)
        
        conn = get_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO dim_location (
                county, city, state, postal_code, census_tract, 
                electric_utility, latitude, longitude
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (county, city, state, postal_code) DO UPDATE SET
                census_tract = EXCLUDED.census_tract,
                electric_utility = EXCLUDED.electric_utility,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude
        """
        
        data_tuples = [tuple(row) for row in df_location.to_numpy()]
        execute_batch(cursor, insert_query, data_tuples, page_size=1000)
        conn.commit()
        
        print(f"Ubicaciones cargadas: {len(df_location)}")
        
        
    except Exception as e:
        conn.rollback()
        print(f"Error cargando dimensión Location: {e}")
        
    finally:
        cursor.close()
        conn.close()

# Carga la tabla dimensional dim_electric_type con tipos de vehículos eléctricos únicos.
# Aplica SCD Tipo 1.
def load_dim_electric_type(data_file_path: str) -> bool:
    
    print("\nCargando dimensión Electric Type...")
    
    try:
        df = pd.read_parquet(data_file_path)
        
        electric_type_column = find_column('electric_vehicle_type', df.columns) or find_column('electric', df.columns)
        if not electric_type_column:
            print("No se encontró columna de tipo eléctrico")
            
        
        unique_types = df[electric_type_column].dropna().astype(str).unique()
        
        conn = get_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO dim_electric_type (electric_type)
            VALUES (%s)
            ON CONFLICT (electric_type) DO UPDATE SET
                electric_type = EXCLUDED.electric_type
        """
        
        data_tuples = [(electric_type,) for electric_type in unique_types]
        execute_batch(cursor, insert_query, data_tuples, page_size=100)
        conn.commit()
        
        print(f"Tipos eléctricos cargados: {len(unique_types)}")
        
        
    except Exception as e:
        conn.rollback()
        print(f"Error cargando dimensión Electric Type: {e}")
        
    finally:
        cursor.close()
        conn.close()

# Carga la tabla dimensional dim_policy con información de elegibilidad CAFV.
# Aplica SCD Tipo 1.
def load_dim_policy(data_file_path: str) -> bool:
   
    print("\nCargando dimensión Policy CAFV...")
    
    try:
        df = pd.read_parquet(data_file_path)
        
        cafv_column = find_column('cafv', df.columns) or find_column('clean_alternative_fuel_vehicle_cafv_eligibility', df.columns)
        if not cafv_column:
            print("No se encontró columna CAFV")
            
        
        unique_policies = df[cafv_column].dropna().astype(str).unique()
        
        conn = get_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO dim_policy (cafv_eligibility)
            VALUES (%s)
            ON CONFLICT (cafv_eligibility) DO UPDATE SET
                cafv_eligibility = EXCLUDED.cafv_eligibility
        """
        
        data_tuples = [(policy,) for policy in unique_policies]
        execute_batch(cursor, insert_query, data_tuples, page_size=100)
        conn.commit()
        
        print(f"Políticas cargadas: {len(unique_policies)}")
        
        
    except Exception as e:
        conn.rollback()
        print(f"Error cargando dimensión Policy: {e}")
        
    finally:
        cursor.close()
        conn.close()

# Carga los mapeos de todas las dimensiones para optimizar las consultas en fact_registration.
# Esto es para que, cuando se esté viendo las tablas dimensiones se puedan analizar más rapido en load_fact_registration
def load_dimension_mappings(cursor) -> dict:
    print("Cargando mapeos de dimensiones...")
    
    # Mapeo de fechas disponibles
    cursor.execute("SELECT id_date FROM dim_date")
    available_dates = set(row[0] for row in cursor.fetchall())
    
    # Rango de años disponibles
    year_range = {
        'min_year': min(int(str(date_id)[:4]) for date_id in available_dates),
        'max_year': max(int(str(date_id)[:4]) for date_id in available_dates)
    }
    
    # Mapeo año -> primer id_date del año
    cursor.execute("SELECT year, MIN(id_date) FROM dim_date GROUP BY year")
    year_to_date_mapping = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Mapeo de vehículos: VIN -> id_vehicle
    cursor.execute("SELECT vin, id_vehicle FROM dim_vehicle")
    vehicle_mapping = dict(cursor.fetchall())
    
    # Mapeo de ubicaciones: (county, city, state) -> id_location
    cursor.execute("SELECT county, city, state, id_location FROM dim_location")
    location_mapping = {(row[0], row[1], row[2]): row[3] for row in cursor.fetchall()}
    
    # Mapeo de tipos eléctricos: electric_type -> id_electric_type
    cursor.execute("SELECT electric_type, id_electric_type FROM dim_electric_type")
    electric_type_mapping = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Mapeo de políticas: cafv_eligibility -> id_policy
    cursor.execute("SELECT cafv_eligibility, id_policy FROM dim_policy")
    policy_mapping = {row[0]: row[1] for row in cursor.fetchall()}
    
    return {
        'available_dates': available_dates,
        'year_range': year_range,
        'year_to_date': year_to_date_mapping,
        'vehicles': vehicle_mapping,
        'locations': location_mapping,
        'electric_types': electric_type_mapping,
        'policies': policy_mapping
    }


# Procesa fila por fila lo que se va a ingresar a la tabla de hechos
# Retorna una Tupla con los datos procesados o None si no se puede procesar
def process_registration_record(row: pd.Series, df_columns: list, mappings: dict) -> tuple:
    try:
        # Obtener VIN y su ID
        vin_col = find_column('vin', df_columns)
        if not vin_col:
            return None
            
        vin = str(row[vin_col]).strip()
        vehicle_id = mappings['vehicles'].get(vin)
        if not vehicle_id:
            return None
        
        # Toma fecha basada en model_year
        model_year_col = find_column('model_year', df_columns) or find_column('year', df_columns)
        if not model_year_col:
            return None
            
        model_year = int(row[model_year_col])
        
        # Ajusta el año al rango válido
        if model_year < mappings['year_range']['min_year']:
            model_year = mappings['year_range']['min_year']
        elif model_year > mappings['year_range']['max_year']:
            model_year = mappings['year_range']['max_year']
        
        # Obtiene date_id
        date_id = mappings['year_to_date'].get(model_year)
        if not date_id:
            date_id = int(f"{model_year}0101")
            if date_id not in mappings['available_dates']:
                date_id = min(mappings['available_dates'])
        
        # procesa la ubicación
        location_columns = {
            'county': find_column('county', df_columns),
            'city': find_column('city', df_columns),
            'state': find_column('state', df_columns)
        }
        
        if None in location_columns.values():
            return None
            
        location_key = (
            str(row[location_columns['county']]).strip(),
            str(row[location_columns['city']]).strip(),
            str(row[location_columns['state']]).strip()
        )
        location_id = mappings['locations'].get(location_key)
        if not location_id:
            return None
        
        # Procesar tipo eléctrico
        electric_type_col = find_column('electric_vehicle_type', df_columns)
        if not electric_type_col:
            return None
            
        electric_type = str(row[electric_type_col]).strip()
        electric_type_id = mappings['electric_types'].get(electric_type)
        if not electric_type_id:
            return None
        
        # Procesar política CAFV
        policy_col = find_column('cafv', df_columns) or find_column('clean_alternative_fuel_vehicle_cafv_eligibility', df_columns)
        if not policy_col:
            return None
            
        policy = str(row[policy_col]).strip()
        policy_id = mappings['policies'].get(policy)
        if not policy_id:
            return None
        
        # Procesar campos opcionales
        electric_range = 0
        electric_range_col = find_column('electric_range', df_columns)
        if electric_range_col:
            try:
                electric_range = int(row[electric_range_col] or 0)
            except:
                electric_range = 0
        
        base_msrp = None
        msrp_col = find_column('base_msrp', df_columns)
        if msrp_col:
            base_msrp = row.get(msrp_col)
        
        dol_vehicle_id = ""
        dol_id_col = find_column('dol_vehicle_id', df_columns)
        if dol_id_col:
            dol_vehicle_id = str(row.get(dol_id_col) or '').strip()
        
        return (vehicle_id, date_id, location_id, electric_type_id, policy_id, electric_range, base_msrp, dol_vehicle_id)
        
    except Exception as e:
        print(f"Error procesando registro: {e}")
        return None

# Carga la tabla de hechos fact_registration con registros de vehículos eléctricos,
def load_fact_registration(data_file_path: str) -> bool:
    print("\nCargando tabla de hechos Registration...")
    
    try:
        df = pd.read_parquet(data_file_path)
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Cargar mapeos de dimensiones
        mappings = load_dimension_mappings(cursor)
        
        # Procesar registros
        processed_records = []
        stats = {'processed': 0, 'skipped': 0, 'date_errors': 0}
        
        for idx, row in df.iterrows():
            record = process_registration_record(row, df.columns, mappings)
            
            if record:
                processed_records.append(record)
                stats['processed'] += 1
                
                if stats['processed'] % 1000 == 0:
                    print(f"Procesados {stats['processed']} registros")
            else:
                stats['skipped'] += 1
        
        # Inserta registros en la base de datos
        if not processed_records:
            print("No hay datos para insertar - verifica las tablas dimensionales")
            
        
        insert_query = """
            INSERT INTO fact_registration (
                id_vehicle, id_date, id_location, id_electric_type, 
                id_policy, electric_range_km, base_msrp_usd, dol_vehicle_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        print(f"Insertando {len(processed_records)} registros...")
        execute_batch(cursor, insert_query, processed_records, page_size=500)
        conn.commit()
        
        print(f"Registros procesados: {stats['processed']}")
        print(f"Registros omitidos: {stats['skipped']}")
        print(f"Registros cargados exitosamente: {len(processed_records)}")
        
        
        
    except Exception as e:
        conn.rollback()
        print(f"Error cargando tabla de hechos: {e}")
        
    finally:
        cursor.close()
        conn.close()