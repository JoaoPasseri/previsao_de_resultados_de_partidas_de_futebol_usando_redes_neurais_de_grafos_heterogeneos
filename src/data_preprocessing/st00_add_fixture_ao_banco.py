import pandas as pd
from typing import Dict
import http.client
import json
import duckdb as duckdb
import requests
from dotenv import load_dotenv
import os
import sys

load_dotenv()

def abrir_arquivo_json(caminho_arquivo: str) -> Dict:

    with open(caminho_arquivo, 'r') as arquivo:
        dados_json = json.load(arquivo)
    return dados_json

def get_football_fixtures(parameters: Dict) -> Dict:
    # Defina os cabeçalhos da requisição
    headers = {
        'x-rapidapi-host': os.getenv('x_rapidapi_host'),
        
        'x-rapidapi-key': os.getenv('x_rapidapi_key')
    }

    # Crie uma conexão com a API
    conn = http.client.HTTPSConnection("v3.football.api-sports.io")

    # Codifique os parâmetros na string da consulta (query string)
    query_string = "&".join([f"{key}={value}" for key, value in parameters.items()])

    # Faça a requisição GET com os cabeçalhos e a string da consulta
    conn.request("GET", "/fixtures?" + query_string, headers=headers)

    # Obtenha a resposta
    res = conn.getresponse()
    data = res.read()

    data = json.loads(data.decode("utf-8"))

    return data

def get_football_lineups(parameters: Dict) -> Dict:
    # Defina os cabeçalhos da requisição
    headers = {
        'x-rapidapi-host': os.getenv('x_rapidapi_host'),
        'x-rapidapi-key': os.getenv('x_rapidapi_key')
    }

    # Crie uma conexão com a API
    conn = http.client.HTTPSConnection("v3.football.api-sports.io")

    # Codifique os parâmetros na string da consulta (query string)
    query_string = "&".join([f"{key}={value}" for key, value in parameters.items()])

    # Faça a requisição GET com os cabeçalhos e a string da consulta
    conn.request("GET", "/fixtures/lineups?" + query_string, headers=headers)

    # Obtenha a resposta
    res = conn.getresponse()
    data = res.read()

    data = json.loads(data.decode("utf-8"))

    return data

def get_football_status() -> Dict:
    # Defina os cabeçalhos da requisição
    headers = {
        'x-rapidapi-host': os.getenv('x_rapidapi_host'),
        'x-rapidapi-key': os.getenv('x_rapidapi_key')
    }

    # Faça a requisição GET com os cabeçalhos e a URL completa
    response = requests.get("https://v3.football.api-sports.io/status", headers=headers)

    # Verifique se a resposta está bem-sucedida
    if response.status_code != 200:
        raise ValueError(f"Erro na requisição: {response.status_code}")

    # Tente decodificar a resposta JSON
    try:
        data = response.json()
    except json.JSONDecodeError as e:
        raise ValueError(f"Erro ao decodificar JSON: {e}")

    return data

class SoccerPipeline:
    def __init__(self, caminho: str):
        self.caminho = caminho

    def conectar_ao_banco_com_duckdb(self):
        con = duckdb.connect(database=self.caminho, read_only=False)
        return con
        
    def query_execute(self, query: str) -> None:
            try:
                con = self.conectar_ao_banco_com_duckdb()
                dataframe = con.execute(query).df()
                return dataframe
            except Exception as e:
                print("Ocorreu um erro ao se comunicar com o banco de dados:", e)
                con.close()
            finally:
                con.close()
            
    def dataframe_to_fixtures(self, data: Dict) -> pd.DataFrame:
        fixtures_data = []

        for match in data['response']:
            fixture = match['fixture']
            league = match['league']
            teams = match['teams']
            goals = match['goals']
            score = match['score']

            fixture_data = {
                'fixture_id': fixture['id'],
                'fixture_referee': fixture['referee'],
                'fixture_timezone': fixture['timezone'],
                'fixture_date': fixture['date'],
                'fixture_timestamp': fixture['timestamp'],
                'fixture_periods_first': fixture['periods']['first'],
                'fixture_periods_second': fixture['periods']['second'],
                'fixture_venue_id': fixture['venue']['id'],
                'fixture_venue_name': fixture['venue']['name'],
                'fixture_venue_city': fixture['venue']['city'],
                'fixture_status_long': fixture['status']['long'],
                'fixture_status_short': fixture['status']['short'],
                'fixture_status_elapsed': fixture['status']['elapsed'],
                'league_id': league['id'],
                'league_name': league['name'],
                'league_country': league['country'],
                'league_logo': league['logo'],
                'league_flag': league['flag'],
                'league_season': league['season'],
                'league_round': league['round'],
                'teams_home_id': teams['home']['id'],
                'teams_home_name': teams['home']['name'],
                'teams_home_logo': teams['home']['logo'],
                'teams_home_winner': teams['home']['winner'],
                'teams_away_id': teams['away']['id'],
                'teams_away_name': teams['away']['name'],
                'teams_away_logo': teams['away']['logo'],
                'teams_away_winner': teams['away']['winner'],
                'goals_home': goals['home'],
                'goals_away': goals['away'],
                'score_halftime_home': score['halftime']['home'],
                'score_halftime_away': score['halftime']['away'],
                'score_fulltime_home': score['fulltime']['home'],
                'score_fulltime_away': score['fulltime']['away'],
                'score_extratime_home': score['extratime']['home'],
                'score_extratime_away': score['extratime']['away'],
                'score_penalty_home': score['penalty']['home'],
                'score_penalty_away': score['penalty']['away']
            }
            fixtures_data.append(fixture_data)
        # Criar DataFrame a partir da lista de dicionários
        df = pd.DataFrame(fixtures_data)
        
        return df
    
    def transform_lineups_to_dataframe(self,data: Dict) -> pd.DataFrame:
        rows = []
        fixture = data['parameters']['fixture']
        for entry in data['response']:
            team_info = entry['team']
            coach_info = entry['coach']
            formation = entry['formation']
            
            # Processar startXI
            for player_entry in entry.get('startXI', []):
                player_info = player_entry['player']
                rows.append({
                    'fixture_id': fixture,  # Adiciona a fixture_id
                    'team_id': team_info['id'],
                    'team_name': team_info['name'],
                    'team_logo': team_info['logo'],
                    'player_id': player_info['id'],
                    'player_name': player_info['name'],
                    'player_number': player_info['number'],
                    'player_pos': player_info['pos'],
                    'player_grid': player_info['grid'],
                    'startXI': 1,  # Indica que o jogador está no startXI
                    'coach_id': coach_info['id'],
                    'coach_name': coach_info['name'],
                    'formation': formation
                })

            # Processar substitutes
            for player_entry in entry.get('substitutes', []):
                player_info = player_entry['player']
                rows.append({
                    'fixture_id': fixture,  # Adiciona a fixture_id
                    'team_id': team_info['id'],
                    'team_name': team_info['name'],
                    'team_logo': team_info['logo'],
                    'player_id': player_info['id'],
                    'player_name': player_info['name'],
                    'player_number': player_info['number'],
                    'player_pos': player_info['pos'],
                    'player_grid': player_info['grid'],
                    'startXI': 0,  # Indica que o jogador está nos substitutos
                    'coach_id': coach_info['id'],
                    'coach_name': coach_info['name'],
                    'formation': formation
                })

        # Criar o DataFrame
        df = pd.DataFrame(rows)
        return df
    
    def adicionar_dados_ao_banco(self, dataframe: pd.DataFrame, tabela: str, if_exists='append') -> None:
        try:
            con = self.conectar_ao_banco_com_duckdb()
            dataframe.to_sql(tabela, con, if_exists= if_exists, index=False)
        except Exception as e:
            print("Ocorreu um erro ao adicionar dados ao banco de dados:", e)
            con.close()
        finally:
            con.close()


if __name__== "__main__":

    # carregando principais ligas
    caminho = os.getenv('MODULE_PATH_DATA') + 'main_leagues.json'
    main_leagues = abrir_arquivo_json(caminho)

    # carregando parametros da requisicao
    caminho = os.getenv('MODULE_PATH_DATA') + 'pull_fixtures.json'
    parameters = abrir_arquivo_json(caminho)

    print(main_leagues)
    print(f'realizando requisicao: {parameters}')

    # realizando requisicao
    data = get_football_fixtures(parameters)

    # Criando instancia de comunicacao com o banco
    caminho = os.getenv('MODULE_PATH_DATA') + 'futebol.db'
    SoccerPipeline = SoccerPipeline(caminho=caminho)

    # Transformando a requisicao no formato da tabela fixture
    data = SoccerPipeline.dataframe_to_fixtures(data)

    # eliminando jogos repetidos
    query = f'''
        SELECT DISTINCT fixture_id
        FROM fixture
        WHERE league_id = {data['league_id'].values[0]};
    '''
    filtro = SoccerPipeline.query_execute(query)
    filtro = filtro['fixture_id']
    filtro = ~data['fixture_id'].isin(filtro)

    print('data shape antes da verificacao de jogos repetidos \n: ', data.shape)
    data = data[filtro]
    print('data shape após da verificacao de jogos repetidos \n: ', data.shape)

    # Adiciona as informacoes ao banco
    if data.shape[0] > 0:
        SoccerPipeline.adicionar_dados_ao_banco(data, 'fixture')
    else:
        print("Todos os dados já foram adicionados anteriormente")