import pandas as pd
from gdrive import *
from log import *

SCOPES = ["https://www.googleapis.com/auth/drive"]

filename = input("Insira o caminho da planilha: ")

df = read_file(filename)

service = login(SCOPES)

n_sequencia = 1
sem_sequencia = []
for trafo, grupo in df.groupby("TRAFO"):
    municipio, equipe, sequencias, intervalos, indexes = get_info(grupo)
    municipio_id = get_id(service, municipio, "folder")
    equipe_id = get_id(service, equipe.capitalize(), "folder", municipio_id)
    trafo_id = get_id(service, trafo, "folder", equipe_id)
    logging.info(f"{municipio} - {equipe} - {trafo}")
    for sequencia, intervalo, index in zip(sequencias, intervalos, indexes):
        n_movidos = 0
        dest_id = create_folder(service, int(n_sequencia), trafo_id)
        for foto in intervalo:
            foto_id = get_id(
                service, foto, "jpeg", trafo_id, municipio, equipe, trafo, sequencia
            )
            if dest_id == trafo_id:
                continue
            if foto_id:
                move_file(service, foto_id, trafo_id, dest_id)
                n_movidos += 1
        update_excel(df, index, n_movidos, n_sequencia)
        n_sequencia += 1
    check_missing(service, trafo_id, municipio, equipe, trafo)

df.to_excel(filename, index=False)
