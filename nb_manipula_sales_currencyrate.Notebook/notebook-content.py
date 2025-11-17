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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Manipular el fichero parquet para transformarlo en tabla

import pandas as pd

df_cr_pandas = pd.read_parquet("/lakehouse/default/Files/sales/currencyrate")

df_cr_pandas = df_cr_pandas.drop(columns=["modifieddate"])

# display(df_cr_pandas)

#castear el dataframe

df_cr = (df_cr
  .withColumn("currencyrateid", F.col("currencyrateid").cast("int"))
  .withColumn("currencyratedate", F.col("currencyratedate").cast("date"))
  .withColumn("fromcurrencycode", F.col("fromcurrencycode").cast("string"))
  .withColumn("tocurrencycode", F.col("tocurrencycode").cast("string"))
  .withColumn("averagerate", F.col("averagerate").cast("double"))
  .withColumn("endofdayrate", F.col("endofdayrate").cast("double"))
)
display(df_cr)

# 2) transformar de pandas a spark

from pyspark.sql import functions as F

df_cr = spark.createDataFrame(df_cr_pandas)

# 3) guardar de spark a tabla en el lakehouse

df_cr.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("Bronce_Landing.sales_currencyrate")

#verificacion

spark.sql("SELECT * FROM Bronce_Landing.sales_currencyrate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
