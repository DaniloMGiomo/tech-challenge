'''
uvicorn app:app --reload
http://127.0.0.1:8000

  "username": "test",
  "password_hash": "test"
'''

from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi import FastAPI
import jwt
from tortoise.contrib.pydantic import pydantic_model_creator
from tortoise.contrib.fastapi import register_tortoise

from vitibrasil_process import validate_date, get_data
from models import *

''' Starting FastAPI framework '''
app = FastAPI(debug=True, title="2MLET vitivinicultura API")

""" ------------------------------------- user management ------------------------------------- """
JWT_SECRET = '2MLET_postech_2024'
User_Pydantic = pydantic_model_creator(User, name='User')
UserIn_Pydantic = pydantic_model_creator(User, name='UserIn', exclude_readonly=True)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')

async def authenticate_user(username: str, password: str):
    user = await User.get(username=username)
    if not user:
        return False 
    if not user.verify_password(password):
        return False
    return user 

async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
        user = await User.get(id=payload.get('id'))
    except:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail='Invalid username or password'
        )
    return await User_Pydantic.from_tortoise_orm(user)

""" ------------------------------------- API's para crawler vitivinicultura ------------------------------------- """

@app.get('/')
async def welcome():
    return {'msg': 'Welcome to 2MLET vitivinicultura API!', 'version': '0'}

@app.post('/token')
async def generate_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail='Invalid username or password'
        )
    user_obj = await User_Pydantic.from_tortoise_orm(user)
    token = jwt.encode(user_obj.dict(), JWT_SECRET)
    return {'access_token' : token, 'token_type' : 'bearer'}

@app.get('/get_producao/{ano}')
async def get_producao(ano: int, token: str = Depends(get_current_user)):
    if validate_date(ano):
        params = {
            'ano' : ano,
            'opcao' : 'opt_02',
            }
        df = get_data(params=params, opt='producao')
        df['IEL'] = 'L'
        return df.to_dict(orient='records')


@app.get('/get_processamento/{subopt}/{ano}')
async def get_processamento(subopt: SuboptProcessamento, ano: int, token: str = Depends(get_current_user)):
    if validate_date(ano):
        params = {
            'ano' : ano,
            'opcao' : 'opt_03',
            }
        subopts = {
            'viniferas' : 'subopt_01',
            'americanas_e_hibridas' : 'subopt_02',
            'uvas_de_mesa' : 'subopt_03',
            'sem_classificacao' : 'subopt_04',
            }
        params['subopcao'] = subopts[subopt]
        df = get_data(params=params, opt='processamento', subopt=subopt.name)
        df['IEL'] = 'L'
        return df.to_dict(orient='records')

@app.get('/get_comercializacao/{ano}')
async def get_comercializacao(ano: int, token: str = Depends(get_current_user)):
    if validate_date(ano):
        params = {
            'ano' : ano,
            'opcao' : 'opt_04',
            }
        df = get_data(params=params, opt='comercializacao')
        df['IEL'] = 'L'
        return df.to_dict(orient='records')

@app.get('/get_importacao/{subopt}/{ano}')
async def get_importacao(subopt: SuboptImportacao, ano: int, token: str = Depends(get_current_user)):
    if validate_date(ano):
        IEL = 'I'
        params = {
            'ano' : ano,
            'opcao' : 'opt_05',
            }
        subopts = {
            'vinhos_de_mesa' : 'subopt_01',
            'espumantes' : 'subopt_02',
            'uvas_frescas' : 'subopt_03',
            'uvas_passas' : 'subopt_04',
            'suco_de_uvas' : 'subopt_05',
            }
        params['subopcao'] = subopts[subopt]
        df = get_data(params=params, opt='importacao', subopt=subopt.name, IEL=IEL)
        return df.to_dict(orient='records')

@app.get('/get_exportacao/{subopt}/{ano}')
async def get_exportacao(subopt: SuboptExportacao, ano: int, token: str = Depends(get_current_user)):
    if validate_date(ano):
        IEL = 'E'
        params = {
            'ano' : ano,
            'opcao' : 'opt_06',
            }
        subopts = {
            'vinhos_de_mesa' : 'subopt_01',
            'espumantes' : 'subopt_02',
            'uvas_frescas' : 'subopt_03',
            'suco_de_uvas' : 'subopt_04',
            }
        params['subopcao'] = subopts[subopt]
        df = get_data(params=params, opt='exportacao', subopt=subopt.name, IEL=IEL)
        return df.to_dict(orient='records')

""" ------------------------------------- FUNCIONAL ------------------------------------- """

register_tortoise(
    app, 
    db_url='sqlite://db.sqlite3',
    modules={'models': ['models']},
    generate_schemas=True,
    add_exception_handlers=True
)
