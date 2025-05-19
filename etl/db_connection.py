import os
import psycopg2
from configparser import ConfigParser
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_base_path():
    if os.environ.get('AIRFLOW_HOME'):
        return os.environ['AIRFLOW_HOME']
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

CONFIG_FILE_PATH = os.path.join(get_base_path(), 'config', 'config.conf')

def load_config():
    parser = ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        logger.error(f"Archivo de configuración no encontrado: {CONFIG_FILE_PATH}")
        raise FileNotFoundError(f"No se encontró el archivo de configuración: {CONFIG_FILE_PATH}")
        
    parser.read(CONFIG_FILE_PATH)

    if 'postgresql' not in parser:
        logger.error("La sección 'postgresql' no existe en el archivo de configuración")
        raise KeyError("La sección 'postgresql' no existe en el archivo de configuración")

    cfg = parser['postgresql']
    return {
        'host':     cfg.get('host', 'localhost'),
        'port':     cfg.get('port', '5432'),
        'user':     cfg.get('user', ''),
        'password': cfg.get('password', ''),
        'database': cfg.get('database', ''),
    }

def get_connection():
    db_config = load_config()
    try:
        return psycopg2.connect(**db_config)
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        return None

def execute_query(query, params=None, commit=False):
    conn = get_connection()
    if not conn:
        return False

    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        if commit:
            conn.commit()
            return True
        else:
            result = cur.fetchall()
            return result
    except Exception as e:
        logger.error(f"Error ejecutando query: {e}")
        if commit:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()