# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1a8f3523-8b2b-4c0f-a371-535fea813249",
# META       "default_lakehouse_name": "dados_imdb",
# META       "default_lakehouse_workspace_id": "44ee6e34-3646-4a64-9fea-9efc7fee0a0f",
# META       "known_lakehouses": [
# META         {
# META           "id": "1a8f3523-8b2b-4c0f-a371-535fea813249"
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
