# Electric Vehicle Data Pipeline

Este repositorio contiene un pipeline de datos para ingestar, limpiar, transformar y almacenar datos de registros de vehículos eléctricos (Electric Vehicle Population Data) en PostgreSQL, orquestado con Apache Airflow.

---

## 📋 Descripción del Proyecto

El pipeline automatiza las siguientes etapas:

1. **Descarga automática** del CSV de datos con Beautiful Soup.
2. **Limpieza** del dataset usando Pandas (tipos de datos, valores nulos, normalización de texto).
3. **Conversión a Parquet** para optimizar lectura/escritura.
4. **Creación de esquema** en PostgreSQL (dimensiones y tabla de hechos).
5. **Carga masiva** de datos desde Parquet en las tablas usando SQLAlchemy y `COPY`.

El modelo de datos está diseñado en **star schema** con tablas de dimensiones (`dim_vehicle`, `dim_date`, `dim_location`, `dim_electric_type`, `dim_policy`) y la tabla de hechos `fact_registration`.

---

## 🚀 Tecnologías y Herramientas

* Python 3.8+
* Pandas, Beautiful Soup
* Parquet (PyArrow)
* PostgreSQL 13
* SQLAlchemy
* Apache Airflow 2.x
* Docker & Docker Compose

---

## 📦 Estructura del Repositorio

```
├── dags/                 # DAG de Airflow
│   └── electric_vehicles_etl.py
├── etl/                  # Módulos de Python
│   ├── db_connection.py
│   ├── data_reader.py
│   ├── data_cleaner.py
│   ├── schema_creator.py
│   └── load_data.py
├── config/
│   └── config.conf      # Parámetros (URL, credenciales)
├── data/                # Archivos temporales (CSV, Parquet)
├── docker-compose.yml   # Orquestación de Airflow y PostgreSQL
├── requirements.txt
└── README.md            # Esta documentación
```

---

## 🛠️ Requisitos Previos

* Docker y Docker Compose instalados
* Puerto `8080` libre en `localhost` (para Airflow UI)
* Puerto `6060` libre en `localhost` (para PgAdmin)
* Puerto `5432` libre en `localhost` (para PostgreSQL)

---

## ⚙️ Cómo ejecutar

1. **Clonar el repositorio**:

   ```bash
   git clone https://github.com/EmmanuelMartinez11/etl_promtior
   ```

2. **Revisar configuración** (opcional):
   * Esta configuración tiene como porposito el facilitar las ejecuciones en la base de datos levantada en docker, por lo que se recomienda modificar las credenciales tanto de docker-compose.yml como de config.conf

3. **Levantar servicios**:

   ```bash
   docker-compose up -d
   ```

   * Esto inicia PostgreSQL en `localhost:5432`,  PgAdmin en `http://localhost:8080` y Airflow en `http://localhost:8080`.

4. **Configurar Airflow**:

   * Usuario: `admin` | Contraseña: `admin`
   * Activa el DAG `electric_vehicles_etl` y ejecuta manualmente la primera ejecución.

5. **Verificar carga de datos**:
  Opcion 1:
   * Conéctate a PostgreSQL (`localhost:5432`, usuario `airflow`, DB `promtior_etl`).
   * Revisa las tablas: `dim_*` y `fact_registration`.
  Opcion 2:
   * Conéctate a PgAdmin (`localhost:6060`, usuario `admin@admin.com`, DB `promtior_etl`).
   * Revisa las tablas: `dim_*` y `fact_registration`. usando las consultas SELECT
