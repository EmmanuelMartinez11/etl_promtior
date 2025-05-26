CREATE TABLE IF NOT EXISTS dim_vehicle (
  id_vehicle        SERIAL PRIMARY KEY,
  vin               VARCHAR(17) NOT NULL UNIQUE,
  model_year        SMALLINT     NOT NULL,
  make              VARCHAR(50)  NOT NULL,
  model             VARCHAR(50)  NOT NULL
);
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
CREATE TABLE IF NOT EXISTS dim_electric_type (
  id_electric_type   SERIAL PRIMARY KEY,
  electric_type      VARCHAR(50) NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS dim_policy (
  id_policy            SERIAL PRIMARY KEY,
  cafv_eligibility     VARCHAR(100) NOT NULL UNIQUE
);
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