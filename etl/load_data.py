import pandas as pd
import os
import time
import re
from datetime import datetime
from psycopg2.extras import execute_batch
from etl.db_connection import get_connection
from datetime import datetime, timedelta


# Ruta relativa al archivo de datos limpios
enabledir = os.path.dirname(__file__)
cleaned_data_path = os.path.normpath(
    os.path.join(enabledir, '..', 'data', 'cleaned_data.parquet')
)

# Funcion para buscar las columnas del archivo cleaned_data.parquet que coincidan con las minusculas y los giones bajos
def find_column(pattern, df_columns):
    """
    Busca columnas en el DataFrame que coincidan con el patrón especificado, 
    ignorando mayúsculas/minúsculas y caracteres especiales.
    Facilita la identificación de columnas independientemente de la nomenclatura exacta.
    """
    pat = re.sub(r'[^a-z0-9]', '', pattern.lower())
    for col in df_columns:
        col_norm = re.sub(r'[^a-z0-9]', '', col.lower())
        if pat in col_norm:
            return col
    return None

# Carga la tabla dimensional dim_vehicle con datos de vehículos.
# Si se encuentra un VIN duplicado, en lugar de fallar, actualizará los otros campos. Basicamente aplica una SCD de tipo 1
def load_dim_vehicle():
    
    print("\nCargando dimensión Vehicle...")
    df = pd.read_parquet(cleaned_data_path)
    col_map = {
        'vin': find_column('vin', df.columns),
        'model_year': find_column('model_year', df.columns) or find_column('year', df.columns),
        'make': find_column('make', df.columns),
        'model': find_column('model', df.columns)
    }
    missing = [k for k, v in col_map.items() if v is None]
    if missing:
        print(f"Columnas faltantes en Vehicle: {missing}")
        return False

    df_v = df[list(col_map.values())].rename(columns={v:k for k,v in col_map.items()})
    df_v = df_v.drop_duplicates('vin').dropna(subset=['vin'])
    df_v['vin'] = df_v['vin'].astype(str).str.strip()

    conn = get_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO dim_vehicle (vin, model_year, make, model)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (vin) DO UPDATE SET
      model_year = EXCLUDED.model_year,
      make = EXCLUDED.make,
      model = EXCLUDED.model
    """
    data = [tuple(x) for x in df_v.to_numpy()]
    try:
        execute_batch(cursor, query, data, page_size=1000)
        conn.commit()
        print(f"Vehículos cargados: {len(df_v)}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error en BD Vehicle: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

# Carga la tabla dimensional dim_date con todas las fechas posibles dentro del rango de años de los vehículos.}
#v Normaliza los años para evitar valores extremos o inválidos
# Calcula atributos adicionales para cada fecha (trimestre, mes, día, fin de semana)
# No aplica una SCD ya que no hay datos que actualziar en las fechas, simplemente no agrega una fecha duplicada
def load_dim_date():
    df = pd.read_parquet(cleaned_data_path)
    min_year = df['model_year'].min()
    max_year = df['model_year'].max()
    
    min_year = max(min_year, 1900)  # Asume que el año mínimo razonable es 1900
    max_year = min(max_year, datetime.now().year)  # Evitar años futuros insexistentes

    # Crea todas las fechas desde el primer día del año mínimo hasta el último día del año máximo
    start_date = datetime(int(min_year), 1, 1)
    end_date = datetime(int(max_year), 12, 31)
    delta = timedelta(days=1)

    conn = get_connection()
    cursor = conn.cursor()

    insert = """
    INSERT INTO dim_date (
      id_date, full_date, year, quarter,
      month, month_name, day, day_of_week, is_weekend
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id_date) DO NOTHING
    """

    current = start_date
    count = 0
    while current <= end_date:
        date_key = int(current.strftime('%Y%m%d'))
        try:
            params = (
                date_key, current.date(), current.year,
                f'Q{(current.month - 1)//3 + 1}', current.month,
                current.strftime('%B'), current.day,
                current.strftime('%A'), current.weekday() >= 5
            )
            cursor.execute(insert, params)
            count += 1
        except Exception as e:
            print(f"Error al insertar fecha {current.date()}: {e}")
        current += delta

    try:
        conn.commit()
        print(f"Fechas cargadas: {count}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error en commit: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


# Carga la tabla dimensional dim_location con datos geográficos de los vehículos.
# Aplica SCD tipo 1: actualiza registros existentes con la misma combinación condado+ciudad+estado+código postal
def load_dim_location():

    print("\nCargando dimensión Location...")
    df = pd.read_parquet(cleaned_data_path)
    col_map = {
        'county': find_column('county', df.columns),
        'city': find_column('city', df.columns),
        'state': find_column('state', df.columns),
        'postal_code': find_column('postal_code', df.columns) or find_column('postal', df.columns) or find_column('zip', df.columns),
        'census_tract': find_column('census_tract', df.columns) or find_column('census', df.columns),
        'electric_utility': find_column('electric_utility', df.columns) or find_column('utility', df.columns),
        'latitude': find_column('latitude', df.columns),
        'longitude': find_column('longitude', df.columns) 
    }
    essential = ['county','city','state']
    miss = [k for k in essential if col_map[k] is None]
    if miss:
        print(f"Faltan columnas en Location: {miss}")
        return False
    df_l = df[list(col_map.values())].rename(columns={v:k for k,v in col_map.items()})
    df_l = df_l.fillna({'state':'WA'}).drop_duplicates()
    df_l['census_tract'] = df_l['census_tract'].astype(str).str.slice(0,25)

    conn = get_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO dim_location
      (county, city, state, postal_code, census_tract, electric_utility, latitude, longitude)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (county, city, state, postal_code) DO UPDATE SET
      census_tract = EXCLUDED.census_tract,
      electric_utility = EXCLUDED.electric_utility,
      latitude = EXCLUDED.latitude,
      longitude = EXCLUDED.longitude
    """
    data = [tuple(x) for x in df_l.to_numpy()]
    try:
        execute_batch(cursor, query, data, page_size=1000)
        conn.commit()
        print(f"Ubicaciones cargadas: {len(df_l)}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error en BD Location: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

# Carga la tabla dimensional dim_electric_type con los tipos de vehículos eléctricos.
# Aplica SCD tipo 1 (en caso de que agreguen un nuevo tipo de auto electrico como un auto con motor de hidrogeno)
def load_dim_electric_type():
    print("\nCargando dimensión Electric Type...")
    df = pd.read_parquet(cleaned_data_path)
    col = find_column('electric_vehicle_type', df.columns) or find_column('electric', df.columns)
    if not col:
        print("No se encontró columna de tipo eléctrico")
        return False
    values = df[col].dropna().astype(str).unique()

    conn = get_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO dim_electric_type (electric_type)
    VALUES (%s)
    ON CONFLICT (electric_type) DO UPDATE SET
      electric_type = EXCLUDED.electric_type
    """
    data = [(v,) for v in values]
    try:
        execute_batch(cursor, query, data, page_size=100)
        conn.commit()
        print(f"Electric types cargados: {len(values)}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error en BD Electric Type: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

# Carga la tabla dimensional dim_policy con la información sobre elegibilidad CAFV.
# Aplica SCD tipo 1 (en caso de que se aplique una nueva politica)
def load_dim_policy():
    print("\nCargando dimensión Policy CAFV...")
    df = pd.read_parquet(cleaned_data_path)
    col = find_column('cafv', df.columns) or find_column('clean_alternative_fuel_vehicle_cafv_eligibility', df.columns)
    if not col:
        print("No se encontró columna CAFV")
        return False
    values = df[col].dropna().astype(str).unique()

    conn = get_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO dim_policy (cafv_eligibility)
    VALUES (%s)
    ON CONFLICT (cafv_eligibility) DO UPDATE SET
      cafv_eligibility = EXCLUDED.cafv_eligibility
    """
    data = [(v,) for v in values]
    try:
        execute_batch(cursor, query, data, page_size=100)
        conn.commit()
        print(f"Policies cargadas: {len(values)}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error en BD Policy: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


# Carga la tabla de hechos fact_registration con los registros de vehículos eléctricos, relacionando todas las dimensiones previamente cargadas.

def load_fact_registration():

    print("\nCargando tabla de hechos...")
    df = pd.read_parquet(cleaned_data_path)

    conn = get_connection()
    cursor = conn.cursor()
    
    # Mapas de claves para dimensiones
    print("Cargando mapeos de dimensiones...")
    cursor.execute("SELECT id_date FROM dim_date")
    available_date_ids = set([r[0] for r in cursor.fetchall()])
    min_year = min(int(str(dk)[:4]) for dk in available_date_ids)
    max_year = max(int(str(dk)[:4]) for dk in available_date_ids)
    cursor.execute("SELECT year, MIN(id_date) FROM dim_date GROUP BY year")
    year_date_map = { r[0]: r[1] for r in cursor.fetchall() }
    cursor.execute("SELECT vin, id_vehicle FROM dim_vehicle")
    vehicle_map = dict(cursor.fetchall())
    cursor.execute("SELECT county, city, state, id_location FROM dim_location")
    location_map = { (r[0],r[1],r[2]): r[3] for r in cursor.fetchall() }
    cursor.execute("SELECT id_electric_type, electric_type FROM dim_electric_type")
    et_map = { r[1]: r[0] for r in cursor.fetchall() }
    cursor.execute("SELECT id_policy, cafv_eligibility FROM dim_policy")
    pol_map = { r[1]: r[0] for r in cursor.fetchall() }

    # Contadores para estadísticas
    processed = 0
    skipped = 0
    date_errors = 0
    
    data = []
    for idx, row in df.iterrows():
        try:
            # Extraer VIN y, si no existe, no se carga la fila
            vin_col = find_column('vin', df.columns)
            if vin_col is None:
                continue
            vin = str(row[vin_col]).strip()
            vid = vehicle_map.get(vin)
            if vid is None:
                skipped += 1
                continue
            
            # Toma una fecha a partir del año del modelo del auto, con esa fecha se para en el rango de fechas para ver si es valor
            # Si es valido, se inserta el registro, si no es valido, se parsea la fecha al primero de enero del año en model_year y si es valida esa fecha se inserta
            model_year_col = find_column('model_year', df.columns) or find_column('year', df.columns)
            if model_year_col is None:
                continue
                
            try:
                model_year = int(row[model_year_col])
                if model_year < min_year:
                    model_year = min_year
                elif model_year > max_year:
                    model_year = max_year
                    
                # Buscar id_date para el añ o
                date_id = year_date_map.get(model_year)
                
                # Si no hay mapeo, usar uno estándar
                if date_id is None:
                    # Intentar construir el 1 de enero
                    date_id = int(f"{model_year}0101")
                    # Verificar si ese id_date existe
                    if date_id not in available_date_ids:
                        # Si no existe, usar el primer id_date disponible
                        date_errors += 1
                        date_id = min(available_date_ids)
                
            except Exception as e:
                print(f"Error procesando año: {e}")
                skipped += 1
                continue
            
            # Se toman las columnas de la locacion y se observa si existe al menos una ya definida en la tabla dim_location, si no existe se saltea el insert
            county_col = find_column('county', df.columns)
            city_col = find_column('city', df.columns)
            state_col = find_column('state', df.columns)
            
            if None in (county_col, city_col, state_col):
                skipped += 1
                continue

            county = str(row[county_col]).strip()
            city = str(row[city_col]).strip()
            state = str(row[state_col]).strip()
            lid = location_map.get((county, city, state))
            
            if lid is None:
                skipped += 1
                continue
            

            et_col = find_column('electric_vehicle_type', df.columns)
            if et_col is None:
                skipped += 1
                continue
                
            et_val = str(row[et_col]).strip()
            etid = et_map.get(et_val)
            
            if etid is None:
                skipped += 1
                continue
            
            pol_col = find_column('cafv', df.columns) or find_column('clean_alternative_fuel_vehicle_cafv_eligibility', df.columns)
            if pol_col is None:
                skipped += 1
                continue
                
            pol_val = str(row[pol_col]).strip()
            polid = pol_map.get(pol_val)
            
            if polid is None:
                skipped += 1
                continue
            
            er_col = find_column('electric_range', df.columns)
            er = 0
            if er_col is not None:
                try:
                    er = int(row[er_col] or 0)
                except:
                    er = 0
            
            msrp_col = find_column('base_msrp', df.columns)
            msrp = None
            if msrp_col is not None:
                msrp = row.get(msrp_col)
            
            dol_id_col = find_column('dol_vehicle_id', df.columns)
            dol_id = ""
            if dol_id_col is not None:
                dol_id = str(row.get(dol_id_col) or '').strip()
            
            data.append((vid, date_id, lid, etid, polid, er, msrp, dol_id))
            processed += 1
            if processed % 1000 == 0:
                print(f"Procesados {processed} registros")
                
        except Exception as e:
            print(f"Error en fila {idx}: {e}")
            continue

    # Informar estadísticas
    print(f"Total procesados: {processed}")
    print(f"Total omitidos: {skipped}")
    print(f"Errores de fechas ajustados: {date_errors}")

    query = """
    INSERT INTO fact_registration (
      id_vehicle, id_date, id_location,
      id_electric_type, id_policy,
      electric_range_km, base_msrp_usd, dol_vehicle_id
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """
    try:
        if not data:
            print("No hay datos para insertar - verifica las tablas dimensionales")
            return False
            
        print(f"Intentando insertar {len(data)} registros...")
        execute_batch(cursor, query, data, page_size=500)
        conn.commit()
        print(f"Registros cargados exitosamente: {len(data)}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error en BD Fact: {e}")
        if data:
            print(f"Ejemplo de datos que causaron el error (primeros 3):")
            for i, d in enumerate(data[:3]):
                print(f"  {i}: {d}")
        return False
    finally:
        cursor.close()
        conn.close()