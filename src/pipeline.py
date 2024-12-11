from etl import pipeline_final_vendas
from loguru import logger



pasta = "dados"
pasta_saida = "dataframe"

pipeline_final_vendas(pasta, pasta_saida)