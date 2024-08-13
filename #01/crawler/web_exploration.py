import pandas as pd
import requests

endpoint = 'http://vitibrasil.cnpuv.embrapa.br/index.php'
date_list = range(1970, 2024)

ano = date_list[0]

opt = {
    'Produção' : 'opt_02',
    'Processamento' : 'opt_03',
    'Comercialização' : 'opt_04',
    'Importação' : 'opt_05',
    'Exportação' : 'opt_06'
}

'''
Produção
'''
params = {
    'ano' : ano,
    'opcao' : 'opt_02'
    }

'''
Processamento
'''

subopt_processamento = {
    'Viníferas' : 'subopt_01',
    'Americanas e híbridas' : 'subopt_02',
    'Uvas de mesa' : 'subopt_03',
    'Sem classificação' : 'subopt_04',
}

tipo = 'Viníferas'

params = {
    'ano' : ano,
    'opcao' : 'opt_03',
    'subopcao' : subopt_processamento[tipo],
    }

'''
Comercialização
'''
params = {
    'ano' : ano,
    'opcao' : 'opt_04'
    }

'''
Importação
'''

subopt_importação = {
    'Vinhos de mesa' : 'subopt_01',
    'Espumantes' : 'subopt_02',
    'Uvas frescas' : 'subopt_03',
    'Uvas passas' : 'subopt_04',
    'Suco de uvas' : 'subopt_05',
    }

tipo = 'Importação'

params = {
    'ano' : ano,
    'opcao' : 'opt_05',
    'subopcao' : subopt_importação[tipo],
    }

'''
Exportação
'''

subopt_exportação = {
    'Vinhos de mesa' : 'subopt_01',
    'Espumantes' : 'subopt_02',
    'Uvas frescas' : 'subopt_03',
    'Suco de uvas' : 'subopt_04',
    }

tipo = 'Exportação'

params = {
    'ano' : ano,
    'opcao' : 'opt_06',
    'subopcao' : subopt_exportação[tipo],
    }

