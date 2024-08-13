import pandas as pd
import numpy as np
import requests
import os

endpoint = 'http://vitibrasil.cnpuv.embrapa.br/index.php'
date_list = range(1970, 2024)

ano = date_list[0]

opt_master = {
    'Produção' : {
        'opt' : 'opt_02',
        'subopt' : None
        },
    'Processamento' : {
        'opt' : 'opt_03',
        'subopt' : {
                'Viníferas' : 'subopt_01',
                'Americanas e híbridas' : 'subopt_02',
                'Uvas de mesa' : 'subopt_03',
                'Sem classificação' : 'subopt_04',
            }
        },
    'Comercialização' : {
        'opt' : 'opt_04',
        'subopt' : None
        },
    'Importação' : {
        'opt' : 'opt_05',
        'subopt' : {
                'Vinhos de mesa' : 'subopt_01',
                'Espumantes' : 'subopt_02',
                'Uvas frescas' : 'subopt_03',
                'Uvas passas' : 'subopt_04',
                'Suco de uvas' : 'subopt_05',
            }
        },
    'Exportação' : {
        'opt' : 'opt_06',
        'subopt' : {
                'Vinhos de mesa' : 'subopt_01',
                'Espumantes' : 'subopt_02',
                'Uvas frescas' : 'subopt_03',
                'Suco de uvas' : 'subopt_04',
            }
        }
    }

'''
params
'''

def get_data(params):
    response = requests.get(endpoint, params=params)
    # 
    dfs = pd.read_html(response.text, attrs={"class":"tb_base tb_dados"}, thousands='.')
    df = dfs[0]
    # 
    colname = [col for col in df.columns if 'Quantidade' in col][0]
    df['Unidade_medida'] = colname.split('Quantidade (')[-1].replace(')', '').replace('.', '')
    df.rename(columns={colname:'Quantidade'}, inplace=True)
    # 
    colname = [col for col in df.columns if 'Valor' in col]
    if len(colname) > 0:
        colname = colname[0]
        df['Unidade_valor'] = colname.split('Valor (')[-1].replace(')', '').replace('.', '')
        df.rename(columns={colname:'Valor'}, inplace=True)
    # 
    df.loc[df['Quantidade'] == '-', 'Quantidade'] = np.nan
    df['opt'] = params['opt']
    df['subopt'] = params['subopt'] if 'subopt' in params else None
    return df

def get_historic_data(params):
    dfs = list()
    for ano in date_list:
        params['ano'] = ano
        response = requests.get(endpoint, params=params)
        # 
        df_ = pd.read_html(response.text, attrs={"class":"tb_base tb_dados"}, thousands='.')
        df = df_[0]
        # 
        colname = [col for col in df.columns if 'Quantidade' in col][0]
        df['Unidade_medida'] = colname.split('Quantidade (')[-1].replace(')', '').replace('.', '')
        df.rename(columns={colname:'Quantidade'}, inplace=True)
        # 
        colname = [col for col in df.columns if 'Valor' in col]
        if len(colname) > 0:
            colname = colname[0]
            df['Unidade_valor'] = colname.split('Valor (')[-1].replace(')', '').replace('.', '')
            df.rename(columns={colname:'Valor'}, inplace=True)
        # 
        df.loc[(df['Quantidade'] == '-') | (df['Quantidade'] == '*'), 'Quantidade'] = np.nan
        df['opt'] = params['opcao']
        subopt = params['subopcao'] if 'subopcao' in params else None
        df['subopt'] = subopt
        df['ano'] = ano
        # 
        dfs.append(df)
        print(f"ano is: {ano}", " opt is: ", params['opcao'], " subopt is: ", subopt)
    df = pd.concat(dfs, ignore_index=True)
    df = df.astype({
        "Quantidade": str
    })
    return df

logging = list()
database = dict()
files = os.listdir()

for params in opt_master.keys():
    settings = opt_master[params]
    opt = settings['opt']
    subopts = settings['subopt']
    # 
    params = {
        'ano' : ano,
        'opcao' : opt,
        }
    # 
    if subopts is not None:
        for subopt in subopts.keys():
            filename = f'opt{opt}_{subopt}.parquet'
            if filename not in files:
                params['subopcao'] = subopts[subopt]
                # df = get_data(params)
                df = get_historic_data(params)
                df.to_parquet(filename)
                # print("opt is: ", opt, " subopt is: ", subopt)
                # df.info()
                # logging.append({'opt' : opt, 'subopt' : subopt, 'columns' : df.columns})
    else:
        filename = f'opt_{opt}.parquet'
        if filename not in files:
            # df = get_data(params)
            df = get_historic_data(params)
            df.to_parquet(filename)
            # print("opt is: ", opt, " subopt is: ", subopt)
            # df.info()
            # logging.append({'opt' : opt, 'subopt' : subopt, 'columns' : df.columns})

cols = ['_'.join(list(col['columns'])) for col in logging]
set(cols)

from pydantic import BaseModel

class Producao(BaseModel):
    Produto: str
    Quantidade: int
    Unidade: str