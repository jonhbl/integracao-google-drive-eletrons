import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
import pandas as pd

import logging
from time import sleep

MIMETYPE_FOLDER = "application/vnd.google-apps.folder"
MIMETYPE_JPEG = "image/jpeg"
RETRIES = 5
DELAYS = [5, 15, 30, 45, 60]

logger = logging.getLogger(__name__)


def get_token(SCOPES: list):
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        logger.info("Token Identificado!")
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            logger.info("Token Expirado!")
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
            logger.info("Token Gerado!")
    return creds


def login(SCOPES: list):
    try:
        creds = get_token(SCOPES)
        service = build("drive", "v3", credentials=creds)
        logger.info("Aplicação logada na API do Google Drive!")
        return service
    except Exception as error:
        logger.critical("Não foi possível logar na API!")


def error_handler(error: dict):
    if error.status_code == 400:
        logger.debug(error)
        logger.error(f"Bad request: {error.reason}")
        return False
    if error.status_code == 401:
        logger.debug(error)
        logger.error(f"Invalid credentials: {error.reason}")
        return False
    if error.status_code == 404:
        logger.debug(error)
        logger.error(f"File not found: {error.reason}")
        return False
    if error.status_code == 403:
        logger.debug(error)
        logger.error(f"Project rate limit exceeded: {error.reason}")
        return True
    if error.status_code == 429:
        logger.debug(error)
        logger.error(f"Too many requests: {error.reason}")
        return True
    if error.status_code == 500:
        logger.debug(error)
        logger.error(f"Backend error: {error.reason}")
        return True
    if error.status_code == 502:
        logger.debug(error)
        logger.error(f"Bad Gateway: {error.reason}")
        return True
    if error.status_code == 503:
        logger.debug(error)
        logger.error(f"Service Unavailable: {error.reason}")
        return True
    if error.status_code == 504:
        logger.debug(error)
        logger.error(f"Gateway Timeout: {error.reason}")
        return False


def get_reponse(service: Resource, query: str):
    response = (
        service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id, name, parents)",
        )
        .execute()
    )
    logger.debug(f"Executando: {query}")
    return response


def search_files(service: Resource, query: str):
    files = []
    counter = 0
    while counter < RETRIES:
        try:
            files = []
            response = get_reponse(service, query)
            files.extend(response.get("files", []))
            logger.debug("Busca realizada com sucesso!")
            return files
        except HttpError as error:
            retry = error_handler(error)
            if not retry:
                break
            delay = DELAYS[counter]
            sleep(delay)
            logger.warning("Esperando {delay}s até tentar novamente...")
            counter += 1
    return None


def get_id(
    service: Resource,
    name: str,
    minetype: str,
    parent_id: str = None,
    municipio: str = None,
    equipe: str = None,
    trafo: str = None,
    sequencia: str = None,
):
    query = get_query(minetype, name, parent_id=parent_id)
    response = search_files(service, query)
    if response:
        id = response[0].get("id", None)
        if id:
            return id
    if municipio:
        logger.warning(
            f"ID do arquivo '{name}' não foi econtrada no caminho {municipio}\{equipe}\{trafo} sequencia: '{sequencia}'!"
        )
    else:
        logger.warning(f"ID do arquivo '{name}' não foi econtrada!")
    return None


def create_folder(service: Resource, name: str, parent_id: str, check: bool = True):
    if check:
        query = f"name='{name}' and '{parent_id}' in parents"
        response = search_files(service, query)
        if response:
            logger.debug(f"Pasta '{name}' já existe!")
            return get_id(service, name, "folder", parent_id)

    file_metadata = {
        "name": str(name),
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder_id = service.files().create(body=file_metadata, fields="id").execute()
    logger.debug(f"Pasta '{name}' foi criada.")
    return folder_id.get("id", None)


def move_file(service: Resource, file_id: str, source_id: str, dest_id: str):
    try:
        response = (
            service.files()
            .update(
                fileId=file_id,
                addParents=dest_id,
                removeParents=source_id,
            )
            .execute()
        )
    except HttpError as error:
        error_handler(error)
    logger.debug(f"Arquivo '{file_id}' movido com sucesso!")
    return response


def get_query(mine: str, name: str, parent_id=None):
    if mine == "folder":
        mimetype = MIMETYPE_FOLDER
        match = "="
    if mine == "jpeg":
        mimetype = MIMETYPE_JPEG
        match = "contains"
    if parent_id:
        return f"mimeType='{mimetype}' and name {match} '{name}' and '{parent_id}' in parents"
    return f"mimeType='{mimetype}' and name {match} '{name}'"


def get_info(df):
    try:
        municipio, equipe = df["LOCALIDADE"].unique()[0].split("_")
        indexes = df.index
        sequencias = df["SEQUÊNCIA"]
        intervalos = df["OBSERVAÇÃO DE FOTOS"]
        intervalos = [
            range(int(intervalo.split("-")[0]), int(intervalo.split("-")[-1]) + 1)
            for intervalo in intervalos
        ]
        return municipio, equipe, sequencias, intervalos, indexes
    except Exception as error:
        logger.debug(error)
        logger.critical("Erro na leitura da planilha!")


def update_excel(df: pd.core.frame.DataFrame, index, n_movidos, n_sequencia):
    df.loc[index, "NUMERO DE FOTOS"] = n_movidos
    df.loc[index, "PASTA - GOOGLE DRIVE"] = n_sequencia


def check_missing(service, parent_id: str, municipio: str, equipe: str, trafo: str):
    query = f"mimeType='{MIMETYPE_JPEG}' and '{parent_id}' in parents"
    response = search_files(service, query)
    if response:
        for file in response:
            name = file.get("name", None)
            logger.warning(
                f"A foto '{name}' no caminho {municipio}\{equipe}\{trafo} não possui sequencia definida!"
            )


def read_file(filename):
    try:
        df = pd.read_excel(filename)
        return df
    except FileNotFoundError:
        logger.critical("Arquivo não encontrado!")
