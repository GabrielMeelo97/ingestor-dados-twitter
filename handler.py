import twitter
import databricks
from os import environ as env


def handler(assunto,data_inicial,data_fim):
    token_databricks = env.get('TOKEN_DATABRICKS')
    ingestor(n_tweets = 20,tema = assunto,start_date=data_inicial,end_date=data_fim)
    job = executa_job(8725524865533,token_databricks)
    response_databricks = verifica_job(job,token_databricks)
    return response_databricks
