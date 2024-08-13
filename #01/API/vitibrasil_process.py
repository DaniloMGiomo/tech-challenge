from fastapi import HTTPException, status
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests

from database_update import UpdateDatabase

# max_date, min_date = get_date_interval()
max_date, min_date = 2023, 1970
endpoint = 'http://vitibrasil.cnpuv.embrapa.br/index.php'

database = UpdateDatabase()

def get_date_interval() -> list[int]:
    """
    Retrieves the date interval from the specified endpoint.

    Returns:
        list[int]: A list containing the maximum and minimum dates.
    """
    endpoint = 'http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_02'
    # 
    response = requests.get(endpoint)
    html = bs(response.text, 'html.parser')
    # 
    labels = html.findAll('input', {"class":"text_pesq"})
    # 
    if len(labels) > 0:
        label = labels[0]
    else:
        raise Exception('date not found')
    # 
    max_date = label.get('max')
    min_date = label.get('min')
    return int(max_date), int(min_date)


def validate_date(date: int):
    """
    Validates a given date by comparing it to the minimum and maximum dates.

    Args:
        date (int): The date to be validated.

    Raises:
        HTTPException: If the date is not within the range of minimum and maximum dates.

    Returns:
        bool: True if the date is within the range, False otherwise.
    """
    if date < min_date or date > max_date:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail='Invalid date, please use a date between {} and {}'.format(min_date, max_date)
        )
    else:
        return True

def request_data(params:dict, opt:str, subopt:str=None, IEL='L'):
    """
    Sends a GET request to the endpoint with the provided parameters and returns a pandas DataFrame.
    
    The function expects the following parameters:
    - params (dict): A dictionary containing the parameters for the GET request.
    - opt (str): A string representing the option for the request.
    - subopt (str): An optional string representing the sub-option for the request. Defaults to None.
    - IEL (str): A string representing the Import/Export Label. Defaults to 'L'.
    
    The function returns a pandas DataFrame containing the data from the response, 
    with the columns renamed and data types converted according to the schema.
    """
    # 
    local_schema = {
        'Produto' : str,
        'Quantidade' : int,
        'Unidade_medida' : str,
        'opt' : str,
        'subopt' : str,
        'ano' : int,
        }
    # 
    ImportExport_schema = {
        'Países' : str,
        'Quantidade' : int,
        'Valor' : int,
        'Unidade_medida' : str,
        'Unidade_valor' : str,
        'opt' : str,
        'Produto' : str,
        'ano' : int,
        }
    # 
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
    df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce', downcast='integer')
    df['opt'] = opt
    df['subopt'] = subopt
    df['ano'] = params['ano']
    df['IEl'] = IEL
    # 
    df.rename(columns={'Cultivar':'Produto', 'Sem definição':'Produto'}, inplace=True)
    # 
    if df.shape[1] == 9:
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce', downcast='integer')
        df.rename(columns={'subopt':'Produto'}, inplace=True)
        schema = ImportExport_schema
    else:
        schema = local_schema
    df.fillna(0, inplace=True)
    df = df.astype(schema)
    return df

def get_data(params:dict, opt:str, subopt:str='0', IEL='L', database=database):
    """
    Retrieves data from the database based on the provided parameters.

    Args:
        params (dict): A dictionary containing the parameters for the data retrieval.
            - 'ano' (int): The year to filter the data by.
        opt (str): The option for the data retrieval.
        subopt (str, optional): The sub-option for the data retrieval. Defaults to '0'.
        IEL (str, optional): The Import/Export Label. Defaults to 'L'.
        database (object, optional): The database object to use for data retrieval. Defaults to the global 'database' object.

    Returns:
        pandas.DataFrame: The retrieved data.

    This function retrieves data from the database based on the provided parameters. It first checks if the data is already present in the database. If not, it requests the data using the 'request_data' function and updates the database accordingly. The retrieved data is then returned.

    Note:
        - The 'database' object must have the following attributes:
            - 'df_local' (pandas.DataFrame): The local database.
            - 'df_ImportExport' (pandas.DataFrame): The import/export database.
            - 'update_database_L' (function): The function to update the local database.
            - 'update_database_IE' (function): The function to update the import/export database.
    """
    if IEL == 'L':
        df = database.df_local.copy()
        df.query("opt == @opt and subopt == @subopt and ano == @params['ano']", inplace=True)
    else:
        df = database.df_ImportExport.copy()
        df.query("opt == @opt and Produto == @subopt and ano == @params['ano']", inplace=True)
    if df.empty:
        print("requesting data")
        df = request_data(params=params, opt=opt, subopt=subopt, IEL=IEL)
        if IEL == 'L':
            database.update_database_L(df=df)
        else:
            database.update_database_IE(df=df)
    return df
