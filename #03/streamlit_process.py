from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import os

path = r'C:\Users\danilo.giomo\Documents\GitHub\danilo\courses\postech - 2MLET\tech challange\#03\rosto'
os.chdir(path)

filename = 'gold_maquiagem.parquet'
parquet_path = os.path.join(path, filename)
df_gold = pd.read_parquet(parquet_path, use_threads=True, engine='pyarrow')

import streamlit as st
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Configuração da página
st.set_page_config(page_title="Busca por Similaridade", layout="wide")

# Corpus de exemplos
corpus = df_gold['descricao']

vectorizer = TfidfVectorizer()
tfidf_matrix = vectorizer.fit_transform(corpus)

# Função 1: Busca por similaridade
def search_similarity(vectorizer, tfidf_matrix, query, corpus, top_n=10):
    query_vector = vectorizer.transform([query])
    similarities = cosine_similarity(query_vector, tfidf_matrix).flatten()
    indices = similarities.argsort()[-top_n:][::-1]
    # 
    unique_results = []
    seen_similarities = set()
    for i in indices:
        sim = similarities[i]
        if sim not in seen_similarities:
            unique_results.append({'Texto': corpus[i], 'Similaridade': sim})
            seen_similarities.add(sim)
    # 
    return pd.DataFrame(unique_results)

def ingest_data(html: BeautifulSoup) -> pd.DataFrame:
    def locate_split(link:str):
        pos_duvida = link.find('?')
        pos_duvida = pos_duvida if pos_duvida > 1 else 999
        pos_hash = link.find('#')
        if pos_duvida < pos_hash:
            return link.split('?')[0]
        else:
            return link.split('#')[0]
    lis = html.find_all('li', {"class":"ui-search-layout__item"})
    # 
    dfs = list()
    for li in lis:
        intermediario = li.find('h2', {"class":"poly-box poly-component__title"})
        # 
        marca = li.find('span', {"class":"poly-component__brand"})
        rating = li.find('span', {"class":"poly-reviews__rating"})
        num_reviews = li.find('span', {"class":"poly-reviews__total"})
        preco =  li.find('span', {"aria-roledescription":"Preço"})
        shipping = li.find('div', {"class":"poly-component__shipping"})
        # 
        li_data = {
            # 'imagem' : li.find('img').get('src'),
            'marca' : None if marca == None else marca.text,
            'descricao' : intermediario.text,
            'link_norm' : locate_split(intermediario.a.get('href')),
            'rating' : None if rating == None else rating.text,
            'num_reviews' : None if num_reviews == None else num_reviews.text,
            'preco' : None if preco == None else preco.text,
            'shipping' : None if shipping == None else shipping.text,
            'date' : datetime.now()
            }
        dfs.append(li_data)
    df = pd.DataFrame(dfs)
    return df

def treatment_data(df:pd.DataFrame):
    alpha = 0.3
    beta = 0.4
    gamma = 0.1
    theta = 0.2
    df.dropna(subset=['rating', 'num_reviews'], inplace=True)
    df['num_reviews'] = df['num_reviews'].apply(lambda x: int(x.replace("(", "").replace(")", "")))
    df['preco'] = df['preco'].apply(lambda x: float(x.replace("R$", "").replace(",", ".")))
    df['rating'] = df['rating'].astype(float)
    df['success_rate'] = (((df['rating'] / 5) * df['num_reviews']) + 1) / (df['num_reviews'] + 2)
    df['shipping'] = df['shipping'].apply(lambda x: 0 if x == None else 1)
    df['maximize'] = alpha*df['success_rate'] - beta*df['preco'] + gamma*df['shipping'] + theta*df['num_reviews']
    df.sort_values(by=['maximize'], ascending=False, inplace=True)
    return df.head(5)

import aiohttp
import asyncio

# Função assíncrona para buscar HTML
async def fetch_html(session, produto):
    url = f'https://lista.mercadolivre.com.br/{produto}'
    async with session.get(url) as response:
        return produto, await response.text()

# Função para atualizar o TF-IDF com novos dados
def update_tfidf(corpus):
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(corpus)
    return vectorizer, tfidf_matrix


# Função principal assíncrona para pegar as recomendações
async def get_recommendation(results):
    dfs = list()
    
    # Inicializa a sessão do aiohttp
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Cria tarefas para cada produto
        for produto in results.Texto.unique():
            tasks.append(fetch_html(session, produto))
        
        # Aguarda todas as requisições assíncronas serem concluídas
        responses = await asyncio.gather(*tasks)
        
        # Processa cada resposta recebida
        for produto, html_text in responses:
            html = BeautifulSoup(html_text, 'html.parser')
            df = ingest_data(html)
            df_top3 = treatment_data(df)
            dfs.append(df_top3)
    
    # Concatena todos os DataFrames processados
    df = pd.concat(dfs)
    df['MLB_ID'] = [x.replace("-", "").split("MLB")[-1][:8] for x in df.link_norm]
    df.query("MLB_ID != '/count'", inplace=True)
    df.sort_values(by=['maximize'], ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    # Atualiza a base de dados local
    try:
        # Carrega o dataset existente
        current_data = pd.read_parquet(parquet_path)
        updated_data = pd.concat([current_data, df]).drop_duplicates(subset=['MLB_ID', 'date'])
    except FileNotFoundError:
        # Caso não exista o arquivo, considera apenas os novos dados
        updated_data = df
    
    # Salva a base atualizada
    updated_data.to_parquet(parquet_path, index=False)
    
    # Recria o corpus para TF-IDF
    corpus = updated_data['descricao']
    vectorizer, tfidf_matrix = update_tfidf(corpus)
    
    df.drop(columns=['maximize', 'shipping'], inplace=True)
    return df, vectorizer, tfidf_matrix

# Função wrapper para rodar o loop de eventos do asyncio
def run_get_recommendation(results):
    return asyncio.run(get_recommendation(results))

# Streamlit App
st.title("Busca por Similaridade de Texto")
st.subheader("Insira um texto para encontrar os mais similares no corpus.")

# Layout com colunas para ajustar largura do input
col1, col2 = st.columns([1, 3])  # Ajuste os valores para controlar proporções

with col1:
    # Input de texto com largura controlada
    query = st.text_input("Digite seu texto:", "")

# Botão para executar a busca
if st.button("Buscar"):
    if query.strip():  # Verifica se o input não está vazio
        # Executa a função 1
        similar_df = search_similarity(vectorizer, tfidf_matrix, query, corpus)
        st.subheader("Resultados de Similaridade:")
        st.dataframe(similar_df)
        
        # Mostra spinner durante o carregamento do segundo DataFrame
        with st.spinner("Carregando informações adicionais..."):
            enriched_df, vectorizer, tfidf_matrix = run_get_recommendation(similar_df)
            names = search_similarity(vectorizer, vectorizer.fit_transform(enriched_df.descricao.unique()), query, enriched_df.descricao, top_n=15).Texto.unique()
            enriched_df.query("descricao in @names")
            
            
        st.subheader("Resultados com Informações Adicionais:")
        st.dataframe(enriched_df, use_container_width=True)  # Alarga o DataFrame
    else:
        st.warning("Por favor, insira um texto para realizar a busca.")
