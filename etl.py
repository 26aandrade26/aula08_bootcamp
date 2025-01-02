import pandas as pd
import os
import glob

# uma funcao de extrat que le e consolida os json

def extrair_dados_e_consolidar(pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

# uma funcao que transforma

def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] *  df["Venda"]
    return df

# uma funcao que faz load dos dados

def carregar_dados(df: pd.DataFrame, format_saida: list):
    """
    parametro que vai ser ou "csv" ou "parquet" ou "os dois"
    """
    for formato in format_saida:
        if formato == 'csv':
            df.to_csv("dados.csv", index=False)
        if formato == 'parquet':
            df.to_parquet("dados.parquet", index=False)

def pipeline_calcular_kpi_de_vendas_consolidado(pasta_argumento: str, formato_de_saida: list):
    data_frame = extrair_dados_e_consolidar(pasta_argumento)
    data_frame_carregar = calcular_kpi_de_total_de_vendas(data_frame)
    carregar_dados(data_frame_carregar, formato_de_saida)


# if __name__ == "__main__":
#     pasta_argumento: str = 'data'
#     data_frame = extrair_dados_e_consolidar(pasta=pasta_argumento)
#     data_frame_carregar = calcular_kpi_de_total_de_vendas(data_frame)
#     formato_de_saida: list = ["csv", "parquet"]
#     carregar_dados(data_frame_carregar, formato_de_saida)