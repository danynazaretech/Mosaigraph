import pandas as pd
import numpy as np
from DadosGeoespacialBR import DadosGeoespacialBR
from GerenciarDadosSMP import GerenciarDadosSMP



class ManipulandoDadosSMP:

    def __init__(self, estacao='[ANATEL]estacoes_smp\\Estacoes_SMP.csv', separador=';'):
        self.smp = GerenciarDadosSMP(estacao,separador)
        self.estacoes = self.smp.get_estacoes_smp()
        self.estacoes = self.estacoes.dropna(subset=['Número Estação'])
        self.estacoes = self.estacoes.drop_duplicates()

    def get_estacoes(self):
        return self.estacoes
    

    def gerar_dados_csv(self):
        uf = DadosGeoespacialBR.estadosBRCSV()
        for info in uf:
            self.estacoes[self.estacoes['UF']==info[1]].to_csv(f'[ANATEL]estacoes_smp\\dado_uf\\Estacoes_{info[1]}.csv', index=False)
        



    def conversao_dados_tratamento(self):
        conversao =['Número Fistel', 'Número Estação', 'Número Ato',   
                    'Código Nacional', 'Código IBGE']
        for convert in  conversao:
            self.estacoes[convert] = self.estacoes[convert].astype('Int64')
            #print(self.estacoes[convert].dtype)
        
        conversao = ['Cep',  'Faixa Estação']
        self.estacoes.loc[(self.estacoes['Faixa Estação'] == 'NÃO IDENTIFICADA') , 'Faixa Estação']='0'#yes
        for convert in  conversao:
            self.estacoes[convert] = self.estacoes[convert].apply(lambda dado: ''.join(filter(str.isdigit, dado)))
            self.estacoes[convert] = self.estacoes[convert].astype('Int64')
            #print(self.estacoes[convert].dtype)
        self.estacoes = self.estacoes.drop_duplicates()

    def deletando_colunas_inoperantes(self):
        colunas_inoperantes = ['NumCnpjCpf','NumServico', 'Situacao','Latitude decimal','Longitude decimal']
        for label in colunas_inoperantes:
            self.estacoes = self.estacoes.drop(label, axis=1)
        self.estacoes = self.estacoes.drop_duplicates()
        print(self.estacoes.columns)

    def apagando_caracteristicas_erb_inoperantes(self):
        self.estacoes = self.estacoes.drop('ClassInfraFisica', axis=1)
        self.estacoes = self.estacoes.drop('NumSetor', axis=1)
        

    def apagando_linhas_duplicadas(self):
       self.estacoes = self.estacoes.drop_duplicates()
    
    def ajustando_geracao_tecnologia_tecnologia5G(self):

        self.estacoes.loc[(self.estacoes['Tecnologia'] == 'EDGE') , 'Geração']='2.5G'#yes
        self.estacoes.loc[((self.estacoes['Tecnologia'].isnull())&(self.estacoes['Geração'].isnull())), ['Tecnologia','Geração','Tipo de Tecnologia 5G']]='INOPERANTE'
        self.estacoes.loc[(self.estacoes['Tecnologia']=='WDCMA'), ['Geração','Tecnologia']]=['3G','WCDMA']
        self.estacoes.loc[(self.estacoes['Tecnologia']=='WCDMA')&(self.estacoes['Tipo de Tecnologia 5G']=='SA-NSA'), 'Tipo de Tecnologia 5G']=None
        self.estacoes.loc[(self.estacoes['Tecnologia']=='LTE')&((self.estacoes['Tipo de Tecnologia 5G']=='SA-NSA') | (self.estacoes['Tipo de Tecnologia 5G']=='NSA')), 'Tipo de Tecnologia 5G']=None
        self.estacoes = self.estacoes.drop_duplicates()
    
    

    
    def quais_enderecos_inconsistentes(self):
        estacao_endereco_inconsistente = []
        estacoes_erb = self.estacoes['Número Estação'].unique().tolist()
        for erb in estacoes_erb:
            radio_base = self.estacoes[self.estacoes['Número Estação']==erb]['Cep'].drop_duplicates()
            if(len(radio_base)>1):
                estacao_endereco_inconsistente.append(erb)
        return estacao_endereco_inconsistente

    def deletando_enderecos_inconsistentes(self, uf='ES'):
        i=0
    
        estacao_uf = pd.read_csv(f'[ANATEL]estacoes_smp\\dado_uf\\Estacoes_{uf}.csv', low_memory=False)
        estacao_uf= estacao_uf.drop('ClassInfraFisica', axis=1)
        estacao_uf = estacao_uf.drop('NumSetor', axis=1)
        estacao_uf = estacao_uf.drop_duplicates()
        
        colunas_inconsistentes = ['Latitude', 'Longitude', 'EnderecoEstacao', 'EndBairro', 
                                        'EndNumero', 'EndComplemento', 'Cep']
        endereco_inconsistente= estacao_uf['Número Estação'].unique()
        
        for station in endereco_inconsistente:
           
            for coluna in colunas_inconsistentes:

                if(not estacao_uf['Número Estação'].empty):
                    if coluna in ['Latitude', 'Longitude']: 
                        estacao_uf.loc[((estacao_uf['Número Estação']==station) & (estacao_uf[coluna].isnull())), coluna]= 0.0 
                    elif coluna in ['Cep']:
                        estacao_uf.loc[((estacao_uf['Número Estação']==station) & (estacao_uf[coluna].isnull())), coluna]= 0
                    else:
                        estacao_uf.loc[((estacao_uf['Número Estação']==station) & (estacao_uf[coluna].isnull())), coluna]= '0'
                    estacao_max =  estacao_uf[estacao_uf['Número Estação']==station][coluna].value_counts().idxmax()
                    #estacao_min = estacoes[estacoes['Número Estação']==station][coluna].value_counts().idxmin()
                    estacao_uf.loc[(estacao_uf['Número Estação']==station), coluna]=estacao_max

            i+=1
            #print(i)
        estacao_uf.to_csv(f'[ANATEL]estacoes_smp\\dado_uf\\dado_{uf}_tratado.csv', index=False)
        #print(estacao_uf.shape)
    
    def atividadesSMP(self, uf='ES', gerarCSV=True):


        self.conversao_dados_tratamento()
        self.deletando_colunas_inoperantes()
        self.ajustando_geracao_tecnologia_tecnologia5G()
        self.estacoes = self.estacoes.drop_duplicates()
        if(gerarCSV==True):
            self.gerar_dados_csv()
        self.apagando_caracteristicas_erb_inoperantes()
        self.deletando_enderecos_inconsistentes(uf='ES')

'''
smp = ManipulandoDadosSMP(estacao='[ANATEL]estacoes_smp\\Estacoes_SMP.csv', separador=';')


print(smp.get_estacoes().shape)
smp.atividadesSMP(gerarCSV=False)
print(smp.get_estacoes().shape)


smp.deletando_enderecos_inconsistentes(uf='AC')

print('AC')'''