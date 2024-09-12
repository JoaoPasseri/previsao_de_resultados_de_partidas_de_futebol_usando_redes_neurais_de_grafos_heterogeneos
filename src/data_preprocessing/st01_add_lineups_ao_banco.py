import st00_add_fixture_ao_banco as st00
from pandas import DataFrame
import time

def main():

    # pegando status atual
    status = st00.get_football_status()
    quantidade_jogos = status['response']['requests']['limit_day'] - status['response']['requests']['current']
    print(f'Ainda restam {quantidade_jogos} requisicoes na API')
    
    # Criando instancia de comunicacao com o banco
    caminho = r'C:\Users\jvpma\Desktop\Soccer Outcomes with Graph 2\futebol.db'
    SoccerPipeline = st00.SoccerPipeline(caminho=caminho)

    # Carregando jogos para acrescentar jogadores
    query = f'''
    SELECT CAST(fixture_id AS INTEGER) AS fixture_id, 
    CAST(goals_home AS INTEGER) AS goals_home,
    CAST(goals_away AS INTEGER) AS goals_away,
    FROM fixture
    WHERE fixture_id NOT IN (SELECT CAST(fixture AS INTEGER) FROM log_lineups)
    AND goals_home IS NOT NULL
    AND goals_away IS NOT NULL
    LIMIT {quantidade_jogos};
    '''
    data = SoccerPipeline.query_execute(query)['fixture_id'].to_list()

    for match in data:
        parameters = {"fixture": str(match),}
        print(parameters)

        # realizando chamadas na API
        df = st00.get_football_lineups(parameters)

        # adicionando log a base de dados
        log = DataFrame({'fixture' : [df['parameters']['fixture']],
                        'erro' : [df['errors']]})
        SoccerPipeline.adicionar_dados_ao_banco(log,'log_lineups')

        # Adicionando as partidas a base de dados
        df = SoccerPipeline.transform_lineups_to_dataframe(df)
        SoccerPipeline.adicionar_dados_ao_banco(df,'lineups')

    # Exibindo uma mensagem de espera
        print("Esperando alguns segundos pra próxima requisição...")
        
        # Aguardar 3 segundos antes da próxima iteração
        time.sleep(1)

if __name__== "__main__":
    main()