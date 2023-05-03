
import re
import io
import os
import pytz
import time
import logging
import psycopg2
import psycopg2 as pgsql
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from time import sleep
from pathlib import Path
from unittest import result
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from airflow.hooks.base import BaseHook
import ftplib

class IntegracaoFunctions:
    ## Construtores da Classe
    def connection():
        conn = BaseHook.get_connection('edg_gcp')
        caminho_relativo = conn.extra_dejson['key_path']
        return caminho_relativo
    
    import ftplib

    def get_ftp_connection_info(ftp_conn):
        """
        Função que recebe o objeto FTP da conexão do Airflow e retorna o host, login e senha.
        """
        host = ftp_conn.host
        login = ftp_conn.login
        password = ftp_conn.password
        
        return host, login, password