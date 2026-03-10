# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9d14d04f-77da-43e9-9a8b-44da24bc5746",
# META       "default_lakehouse_name": "dados_imdb",
# META       "default_lakehouse_workspace_id": "47c4d10a-cb04-40a1-831c-a9fb659d9b5f",
# META       "known_lakehouses": [
# META         {
# META           "id": "9d14d04f-77da-43e9-9a8b-44da24bc5746"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd

id = '1Xts466tVzt92khKBnbuF0A5cIMZqpPJY'
url = f'https://drive.google.com/uc?id={id}'

df_imdb = pd.read_csv(url)

# converter para spark
spark_df = spark.createDataFrame(df_imdb)

# salvar como tabela no lakehouse
spark_df.write.mode("overwrite").saveAsTable("imdb_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
