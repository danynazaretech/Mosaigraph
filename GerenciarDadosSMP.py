import pandas as pd

class GerenciarDadosSMP:
    def __init__(self, estacao='[ANATEL]estacoes_smp\\Estacoes_SMP.csv', separador=';'):
       self.estacoes_smp = pd.read_csv(estacao, sep=separador, low_memory=False)
       self.dados = lambda uf: f'[ANATEL]estacoes_smp/estacoes_smp_uf/dado_{uf}.csv'

    @staticmethod
    def contagem_dados_estacoes(dados_smp):
        print(f'|----------Campo----------|----------Dados Diferentes----------|----------Celulas Vazias----------|')
        print(f'|-------------------------------------------------------------------------------------------------|')
        for info in dados_smp.columns:
            print(f'|----------{info}:----------{len(dados_smp[info].unique())}----------|----------{dados_smp[info].isna().sum()}----------|')
    

    @staticmethod
    def estadosBRJSON():
        estados_br_json = {'Sul':[("Paraná","PR","Curitiba", "4106902",41 ),
                                       ("Santa Catarina","SC","Florianópolis", "2204006",43),
                                       ("Rio Grande do Sul","RS", "Porto Alegre", "4314902",43)],
              'Sudeste':[("Minas Gerais","MG","Belo Horizonte","3106200",31),
                         ("Espírito Santo","ES", "Vitória", "3205309",32),
                         ("Rio de Janeiro","RJ","Rio de Janeiro", "3304557",33),
                         ("São Paulo","SP", "São Paulo","3550308",35) ],
              'Norte':[
                       ("Rondônia","RO","Porto Velho","1100205",11),
                       ("Acre","AC","Rio Branco","5107206",12 ),
                       ("Amazonas","AM", "Manaus", "1302603",13),
                       ("Roraima","RR", "Boa Vista","2502151",14),
                       ("Pará","PA","Belém","2700805",15),
                       ("Amapá","AP", "Macapá","1600303",16),
                       ("Tocantins","TO","Palmas","1721000",17)],
              'Nordeste':[("Maranhão","MA", "São Luís","2111300",21),
                          ("Piauí","PI", "Teresina","2211001",22),
                          ("Ceará","CE", "Fortaleza", "2304400",23),
                          ("Rio Grande do Norte","RN", "Natal","2408102",24),
                          ("Paraíba","PB", "João Pessoa","2507507",25),
                          ("Pernambuco","PE", "Recife","2611606",26),
                          ("Alagoas","AL","Maceió", "2704302",27),
                          ("Sergipe","SE", "Aracaju", "2800308",28),
                          ("Bahia","BA", "Salvador", "2927408",29)
                          ],
              'Centro-Oeste':[
                              ("Mato Grosso do Sul","MS", "Campo Grande","2401305",52),
                              ("Mato Grosso","MT", "Cuiabá", "5103403",51),
                              ("Goiás","GO", "Goiânia", "5208707",52),
                              ("Distrito Federal","DF", "Brasília", "5300108",53)]}
        return estados_br_json
        
    @staticmethod
    def estadosBRCSV():
        estados_br_csv = [
            ("Acre","AC","Rio Branco","5107206" ,"Norte",12),
            ("Alagoas","AL","Maceió", "2704302","Nordeste",27),
            ("Amazonas","AM", "Manaus", "1302603","Norte",13),
            ("Amapá","AP", "Macapá","1600303","Norte",16),
            ("Bahia","BA", "Salvador", "2927408","Nordeste",29),
            ("Ceará","CE", "Fortaleza", "2304400","Nordeste",23),
            ("Distrito Federal","DF", "Brasília", "5300108","Centro-Oeste",53),
            ("Espírito Santo","ES", "Vitória", "3205309","Sudeste",32),
             ("Goiás","GO", "Goiânia", "5208707","Centro-Oeste",52),
             ("Maranhão","MA", "São Luís","2111300","Nordeste",21),
             ("Mato Grosso","MT", "Cuiabá", "5103403","Centro-Oeste",51),
             ("Mato Grosso do Sul","MS", "Campo Grande","2401305","Centro-Oeste",50),
             ("Minas Gerais","MG","Belo Horizonte","3106200","Sudeste",31),
             ("Pará","PA","Belém","2700805","Norte",15),
             ("Paraíba","PB", "João Pessoa","2507507","Nordeste",25),
             ("Paraná","PR","Curitiba", "4106902","Sul",41),
             ("Pernambuco","PE", "Recife","2611606","Nordeste",26),
             ("Piauí","PI", "Teresina","2211001","Nordeste",22),
             ("Rio de Janeiro","RJ","Rio de Janeiro", "3304557","Sudeste",33),
             ("Rio Grande do Norte","RN", "Natal","2408102","Nordeste",24),
             ("Rio Grande do Sul","RS", "Porto Alegre", "4314902","Sul",43),
             ("Rondônia","RO","Porto Velho","1100205","Norte",11),
             ("Roraima","RR", "Boa Vista","2502151","Norte",14),
             ("Santa Catarina","SC","Florianópolis", "2204006","Sul",42),
             ("São Paulo","SP", "São Paulo","3550308","Sudeste",35),
             ("Sergipe","SE", "Aracaju", "2800308","Nordeste",28),                  
             ("Tocantins","TO","Palmas","1721000","Norte",17)]
        return estados_br_csv

    
    def caracteristicas_dados_estacoes(self):
        self.estacoes_smp.head(1000).to_csv('arquivo.csv', index='False')
        return self.estacoes_smp.columns, self.estacoes_smp.shape 


    def organizar_cod_nacional(self):
        ler = lambda uf: f'[ANATEL]estacoes_smp/estacoes_smp_uf/dado_{uf}.csv'
        salvar = lambda uf: f'codigo_nacional/codigo_nacional_{uf}.csv'
        
        codigo_smp = None
        for uf in GerenciarDadosSMP.estadosBRCSV():
            self.estacoes_smp = pd.read_csv(ler(uf[1]), sep=',', low_memory=False)
            self.estacoes_smp[['Código Nacional', 'Código IBGE']].drop_duplicates().to_csv(salvar(uf[1]),index=False)
