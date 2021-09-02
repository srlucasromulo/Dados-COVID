import pandas as pd
import numpy as np
import os


DIR_PREFIX = 'HIST_PAINEL_COVIDBR'
COLUMN_NAMES = [
    'regiao',
    'estado',
    'municipio',
    'coduf',
    'codRegiaoSaude',
    'data',
    'semanaEpi',
    'populacaoTCU2019',
    'casosAcumulado',
    'casosNovos',
    'obitosAcumulado',
    'obitosNovos',
    'Recuperadosnovos',
    'emAcompanhamentoNovos',
    'interior/metropolitana'
]


def load_dir_filenames():

    dit_list = os.listdir()

    for d in dit_list:
        if DIR_PREFIX in d:
            directory = d + '/'

    filenames = sorted(os.listdir(directory))

    return directory, filenames


def load_update_files():

    directory, filenames = load_dir_filenames()
    filename = filenames.pop()
    file = pd.read_csv(directory+filename, sep=';', encoding='UTF-8')
    return file


def load_createDB_files():

    directory, filenames = load_dir_filenames()
    files = []
    for f in filenames:
        files.append(pd.read_csv(directory+f, sep=';', encoding='UTF-8'))
    return files
