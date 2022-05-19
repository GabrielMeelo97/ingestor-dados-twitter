from dags.functions.dados_twitter.databricks import *
from dags.functions.dados_twitter.twitter import *
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def handler(assunto,data_inicial,data_fim):
    token_databricks = env['tokendatabricks']
    ingestor(n_tweets = 20,tema = assunto,start_date=data_inicial,end_date=data_fim)
    job = executa_job(8725524865533,token_databricks)
    response_databricks = verifica_job(job,token_databricks)
    return response_databricks





