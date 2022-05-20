import requests
from datetime import datetime
import json
import boto3
import pandas as pd
import random
import logging
from backoff import on_exception, expo
import ratelimit
from os import environ as env
    

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



def create_header(bearer_token):
    token_body = {'Authorization':bearer_token}
    return token_body

def create_url(tema,start_date,end_date):
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
    filters = f"start_time={start_date}T00:00:00.00Z&end_time={end_date}T23:59:59.00Z"
    query = tema
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(query, tweet_fields, user_fields, filters)
    return url



@on_exception(expo, ratelimit.exception.RateLimitException, max_tries=10)
@ratelimit.limits(calls=29, period=30)
@on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
def request_get(url,token):
    response = requests.get(url,headers=token)
    return response.json()

def paginate(resultado,url):
    if resultado['meta']:
        url =  f"{url}&next_token={resultado['meta']['next_token']}"
    else:
        url = False
    return url

def save_bucket(json_arquivo,aws_account,aws_key):
    now = datetime.now().strftime("%Y-%m-%d")
    json_arquivo = json.dumps(json_arquivo['data'],sort_keys=True)
    id_aws = aws_account
    chave_aws = aws_key
    session = boto3.session.Session(aws_access_key_id=id_aws, 
                                        aws_secret_access_key=chave_aws, region_name= 'sa-east-1')
    s3 = session.resource('s3')
    logger.info(f"Salvando json no bucket..")
    s3.Bucket('dados-twitter').put_object(Key=f'bronze/landing-date={now}/twitter_{random.randint(1,100000000000000000000000000000)}.json', Body=str(json_arquivo))
    return 'Arquivo inserido'

def ingestor(n_tweets,tema,start_date,end_date):
    bearer_token = env.get('twittertoken')
    aws_account = env.get('awsid')
    aws_key = env.get('awskey')
    token = create_header(bearer_token)
    url_base = create_url(tema,start_date,end_date)
    resultado = request_get(url_base,token)
    validacao_paginacao = paginate(resultado,url_base)
    contador = 1
    logger.info(f"Iniciando coleta..")
    for i in range(1,n_tweets+1):
            print (i)
            if validacao_paginacao != False:
                resultado = request_get(validacao_paginacao,token)
                validacao_paginacao = paginate(resultado,url_base)
                contador= contador + 1
                save_bucket(resultado,aws_account,aws_key)
            else:
                logger.info(f"tweets coletados com sucesso")
                break


