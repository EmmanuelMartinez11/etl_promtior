# Módulo para crear las tablas de la base de datos una por una si no existen
# Para crear cada tabla se siguó una logica especifica
from ingest_data.utils.db_connection import execute_query

# 1) Dimensión Vehículo -> Se creó esta tabla dimesion ya que presenta atributos que no cambian mucho von el tiempo, como la marca o el modelo
CREATE_DIM_VEHICLE = """
CREATE TABLE IF NOT EXISTS dim_vehicle (
  id_vehicle        SERIAL PRIMARY KEY,
  vin               VARCHAR(17) NOT NULL UNIQUE,
  model_year        SMALLINT     NOT NULL,
  make              VARCHAR(50)  NOT NULL,
  model             VARCHAR(50)  NOT NULL
);
"""

# 2) Dimensión Fecha -> Necesaria para cualquier etl
CREATE_DIM_DATE = """
CREATE TABLE IF NOT EXISTS dim_date (
  id_date         INTEGER   PRIMARY KEY,
  full_date       DATE      NOT NULL UNIQUE,
  year            SMALLINT  NOT NULL,
  quarter         CHAR(2)   NOT NULL,
  month           SMALLINT  NOT NULL,
  month_name      VARCHAR(9) NOT NULL,
  day             SMALLINT  NOT NULL,
  day_of_week     VARCHAR(9) NOT NULL,
  is_weekend      BOOLEAN   NOT NULL
);
"""

# 3) Dimensión Ubicación -> Se creó esta tabla dimesion ya que presenta atributos que no cambian mucho von el tiempo y además sirve si queremos hacer analisis de ciudades
CREATE_DIM_LOCATION = """
CREATE TABLE IF NOT EXISTS dim_location (
  id_location      SERIAL PRIMARY KEY,
  county           VARCHAR(50) NOT NULL,
  city             VARCHAR(50) NOT NULL,
  state            CHAR(10)    NOT NULL DEFAULT 'WA',
  postal_code      VARCHAR(10) NOT NULL,
  census_tract     VARCHAR(25) NOT NULL,
  electric_utility VARCHAR(250) NOT NULL,
  latitude         NUMERIC(9,6), 
  longitude        NUMERIC(9,6),
  UNIQUE (county, city, state, postal_code)
);
"""

# 4) Dimensión Tipo de Vehículo Eléctrico -> Presenta pocos valores posibles (2) que se repiten en muchos registros
CREATE_DIM_ELECTRIC_TYPE = """
CREATE TABLE IF NOT EXISTS dim_electric_type (
  id_electric_type   SERIAL PRIMARY KEY,
  electric_type      VARCHAR(50) NOT NULL UNIQUE
);
"""

# 5) Dimensión Política CAFV -> Representa una clasificacion de cada auto electrico segun politicas CAF por lo que solo si se modifica o agrega una politica esta tabla va a camboar
CREATE_DIM_POLICY = """
CREATE TABLE IF NOT EXISTS dim_policy (
  id_policy            SERIAL PRIMARY KEY,
  cafv_eligibility     VARCHAR(100) NOT NULL UNIQUE
);
"""

# 6) Tabla de Hechos de Registro -> Data Warehouse guarda los eventos medibles, por ejemplo, "el vehículo VIN XXXXX fue registrado en tal lugar, en tal fecha, con tales características"
CREATE_FACT_REGISTRATION = """
CREATE TABLE IF NOT EXISTS fact_registration (
  id_reg               SERIAL PRIMARY KEY,
  id_vehicle           INTEGER  NOT NULL
     REFERENCES dim_vehicle(id_vehicle),
  id_date              INTEGER  NOT NULL
     REFERENCES dim_date(id_date),
  id_location          INTEGER  NOT NULL
     REFERENCES dim_location(id_location),
  id_electric_type     INTEGER  NOT NULL
     REFERENCES dim_electric_type(id_electric_type),
  id_policy            INTEGER  NOT NULL
     REFERENCES dim_policy(id_policy),
  electric_range_km    SMALLINT,
  base_msrp_usd        NUMERIC(12,2),
  dol_vehicle_id       VARCHAR(20)
);
"""
#  Ejecuta una sentencia CREATE TABLE IF NOT EXISTS.
def create_table(query) -> bool:
    result = execute_query(query, commit=True)
    return result  # Ahora result es True o False

# Crea todas las tablas en orden, evitando duplicados 
def create_all_tables() -> None:
    tables = [
        ('dim_vehicle', CREATE_DIM_VEHICLE),
        ('dim_date', CREATE_DIM_DATE),
        ('dim_location', CREATE_DIM_LOCATION),
        ('dim_electric_type', CREATE_DIM_ELECTRIC_TYPE),
        ('dim_policy', CREATE_DIM_POLICY),
        ('fact_registration', CREATE_FACT_REGISTRATION),
    ]

    for name, query in tables:
        ok = create_table(query)
        if ok:
            print(f"Tabla '{name}' creada o ya existía.")
        else:
            print(f"Error al procesar la tabla '{name}'.")
