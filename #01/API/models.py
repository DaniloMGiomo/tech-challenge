from tortoise.models import Model 
from tortoise import fields 
from passlib.hash import bcrypt
from enum import Enum

class User(Model):
    id = fields.IntField(pk=True)
    username = fields.CharField(50, unique=True)
    password_hash = fields.CharField(128)
    def verify_password(self, password):
        return bcrypt.verify(password, self.password_hash)

class SuboptProcessamento(str, Enum):
    viniferas = "viniferas"
    americanas_e_hibridas = "americanas_e_hibridas"
    uvas_de_mesa = "uvas_de_mesa"
    sem_classificacao = "sem_classificacao"

class SuboptImportacao(str, Enum):
    vinhos_de_mesa = "vinhos_de_mesa"
    espumantes = "espumantes"
    uvas_frescas = "uvas_frescas"
    uvas_passas = "uvas_passas"
    suco_de_uvas = "suco_de_uvas"

class SuboptExportacao(str, Enum):
    vinhos_de_mesa = 'vinhos_de_mesa'
    espumantes = 'espumantes'
    uvas_frescas = 'uvas_frescas'
    suco_de_uvas = 'suco_de_uvas'
