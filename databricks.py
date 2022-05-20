import json
import requests
import time
import logging
from backoff import on_exception, expo
import ratelimit
    

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@on_exception(expo, ratelimit.exception.RateLimitException, max_tries=10)
@ratelimit.limits(calls=29, period=30)
@on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
def executa_job(job_id,token_databricks):
    logger.info(f"Iniciando job databricks")
    url = 'https://dbc-c26d8097-f2f9.cloud.databricks.com/api/2.1/jobs/run-now'
    bearer_token = token_databricks
    body = {
            "job_id":job_id}
    response = requests.post(url,headers={'Authorization':bearer_token},json = body)
    cod_run = json.loads(response.text)
    return cod_run['run_id']

def resultado_job(run_id,token_databricks):
    logger.info(f"Verificando resultado do job em execução")
    url_saida = f'https://dbc-c26d8097-f2f9.cloud.databricks.com/api/2.1/jobs/runs/get-output?run_id={run_id}'
    bearer_token = token_databricks
    response = requests.get(url_saida,headers={'Authorization':bearer_token})
    result = json.loads(response.text)
    return result['metadata']['state']


def verifica_job(run_id):
    for i in range(1,8):
        print("rodando job..")
        retorno = dict(resultado_job(run_id))
        print(retorno['state_message'])
        if retorno['life_cycle_state'] == 'TERMINATED':
            if retorno['result_state'] == 'SUCCESS':
                logger.info(f"Job executado com sucesso..")
                return "Ok"
            else:
                if i == 7:
                    logger.info(f"A execução do job falhou..")
                    return "Error timeout"
        else:
            time.sleep(60)
