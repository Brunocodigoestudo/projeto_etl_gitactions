import pandas as pd
import os
import sys
import glob
from loguru import logger 

logger.add("logs/app.log", level="INFO")

#extract

pasta = "dados"
def extração(pasta:str)-> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_consolidado = pd.concat(df_list, ignore_index=True)
    return df_consolidado

# transformação

def transformação_calculo(df: pd.DataFrame)-> pd.DataFrame:
   df["Total"] = df["Quantidade"] * df["Venda"]
   return df


#load

def carregar_dados(pasta_saida: str, df: pd.DataFrame) -> str:
    os.makedirs(pasta_saida, exist_ok=True)  # Cria a pasta de saída, se não existir
    caminho_arquivo = os.path.join(pasta_saida, 'df_consolidado.csv')
    df.to_csv(caminho_arquivo, index=False)
    return caminho_arquivo


#pipeline

def pipeline_final_vendas(pasta: str, pasta_saida: str):
    data_frame = (extração(pasta))
    transform = (transformação_calculo(data_frame))
    df_consolidado = carregar_dados(pasta_saida,transform)
    logger.info("A pipeline executada com sucesso")
    

    
