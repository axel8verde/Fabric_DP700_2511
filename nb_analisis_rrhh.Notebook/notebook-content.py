# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b33b4eb3-98ab-45ed-8c99-627357f79c2f",
# META       "default_lakehouse_name": "Bronce_Landing",
# META       "default_lakehouse_workspace_id": "e86c0172-5cc1-4296-9188-0a8fc683996a",
# META       "known_lakehouses": [
# META         {
# META           "id": "b33b4eb3-98ab-45ed-8c99-627357f79c2f"
# META         },
# META         {
# META           "id": "38220dde-b32d-4816-8708-f458b843c673"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Trabajamos las tablas sin tiempo complejo (materializar parquets en Tablas)

# CELL ********************

# Leer los ficheros parquet que son el resultado de la importación anterior

df_department = spark.read.parquet("Files/humanresources/department")
df_employeedh = spark.read.parquet("Files/humanresources/employeedepartmenthistory")
df_employeeph = spark.read.parquet("Files/humanresources/employeepayhistory")
df_jobcandidate = spark.read.parquet("Files/humanresources/jobcandidate")
# df_shift = spark.read.parquet("Files/humanresources/shift")

display(df_department)
display(df_employeedh)
display(df_employeeph)
display(df_jobcandidate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Guardar los dataframes como tablas
df_department.write.format("delta").mode("overwrite").saveAsTable("Bronce_Landing.hr_department")
df_employeedh.write.format("delta").mode("overwrite").saveAsTable("Bronce_Landing.hr_employeedepartmenthistory")
df_employeeph.write.format("delta").mode("overwrite").saveAsTable("Bronce_Landing.hr_employeepayhistory")
df_jobcandidate.write.format("delta").mode("overwrite").saveAsTable("Bronce_Landing.hr_jobcandidate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Trabajamos la tabla mas compleja


# CELL ********************

import pandas as pd
from pyspark.sql import functions as F

#1) lee y limpia
df_shift_pandas = pd.read_parquet("/lakehouse/default/Files/humanresources/shift")
df_shift_pandas = df_shift_pandas.drop(columns=["modifieddate"])

#2) Normaliza tipos: time -> string
for c in ["starttime", "endtime"]:
    df_shift_pandas[c] = df_shift_pandas[c].astype(str) # "HH:MM:SS"

#3) de pandas a spark
df_shift = spark.createDataFrame(df_shift_pandas)

#4) Casts finales
df_shift = (df_shift
    .withColumn("shiftid", F.col("shiftid").cast("int"))
    .withColumn("name", F.col("name").cast("string"))
    .withColumn("starttime", F.col("starttime").cast("string"))
    .withColumn("endtime", F.col("endtime").cast("string"))
)

#5) Guarda la tabla delta en Bronce
df_shift.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("Bronce_Landing.hr_shift")

#Verificacion
spark.sql("SELECT * FROM Bronce_Landing.hr_shift").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creación de tablas de hechos y Dimensiones para guardarlas en Silver

# CELL ********************

# Crear la tabla de hechos
df_ft_costos_rrhh = spark.sql(
"""
SELECT
  edh.businessentityid,
  edh.departmentid,
  edh.shiftid,
  eph.ratechangedate,
  eph.rate,
  eph.payfrequency,
  CASE
    WHEN eph.payfrequency = 1 THEN eph.rate / 30   -- Mensual
    WHEN eph.payfrequency = 2 THEN eph.rate / 15   -- Quincenal
    ELSE eph.rate
  END AS costo_dia
FROM Bronce_Landing.hr_employeedepartmenthistory edh
LEFT JOIN Bronce_Landing.hr_employeepayhistory eph
  ON edh.businessentityid = eph.businessentityid
ORDER BY
  edh.businessentityid,
  eph.ratechangedate;

"""
)
display(df_ft_costos_rrhh)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Guardar la tabla de hechos

df_ft_costos_rrhh.write.format("delta").mode("overwrite").saveAsTable("Silver_Refined.ft_costos_rrhh_dia_turno_depto_business_entity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
