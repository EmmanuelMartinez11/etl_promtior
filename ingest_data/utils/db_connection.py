# Modulo para facilitar la conexion con la base de datos
import psycopg2
from configparser import ConfigParser
import os

# Funcion que me permite ejecutar tanto en entorno local como en Airflow
def get_base_path():
    # Si estamos en Airflow, usará /etl como ruta base
    if os.environ.get('AIRFLOW_HOME'):
        return '/etl'
    # En local, usa la ruta relativa desde este archivo
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Se arman las rutas a los archivos de configuración
CONFIG_FILE_PATH = os.path.join(get_base_path(), 'config', 'config.conf')
DB_MOTOR = os.path.join(get_base_path(), 'config', 'motor.txt')

# Se lee el motor de base de datos
try:
    with open(DB_MOTOR, 'r') as archivo:
        contenido = archivo.read().strip()
        print(f"Motor de base de datos: {contenido}")
except FileNotFoundError:
    print(f"Error: El archivo '{DB_MOTOR}' no fue encontrado.")
    contenido = "postgresql" 
except Exception as e:
    print(f"Ocurrió un error al leer el archivo: {e}")
    contenido = "postgresql" 

# Carga el archivo de configuracion
def load_config():
    parser = ConfigParser()
    parser.read(CONFIG_FILE_PATH)
    
    # Si estamos en Airflow, usar la configuración de BD de Airflow
    if os.environ.get('AIRFLOW_HOME'):
        if 'postgresql_airflow' in parser:
            config = parser['postgresql_airflow']
        else: # Usar config local
            config = parser[contenido]  
    else:
        config = parser[contenido]  # Configuración local
        
    return {
        'host': config['host'],
        'user': config['user'],
        'password': config['password'],
        'database': config['database'],
        'port': config.get('port', '5432')
    }

# Crea un objeto conexion
def get_connection():
    db_config = load_config()
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# Ejecuta una query
def execute_query(query, params=None, commit=False):
    conn = get_connection()
    if not conn:
        return False  # Fallo de conexión

    try:
        cur = conn.cursor()
        cur.execute(query, params)
        if commit:
            conn.commit()
            success = True
        else:
            success = True  # Éxito en ejecución (SELECT)
            result = cur.fetchall()
        cur.close()
        return success if commit else result
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        return False
    finally:
        if conn:
            conn.close()