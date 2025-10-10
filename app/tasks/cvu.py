import os
import sys
import requests
import pandas as pd
import numpy as np
from io import StringIO
from bs4 import BeautifulSoup
import datetime
from app.TasksInterface import TasksInterface
from middle.utils import html_to_image
from middle.message import send_whatsapp_message
from middle.utils import setup_logger, Constants, get_auth_header, sanitize_string, convert_date_columns
from middle.airflow import trigger_dag

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.constants import MAPEAMENTO_CVU

logger = setup_logger()
constants = Constants()

class Cvu(TasksInterface):
    
    def __init__(self):
        self.read_cvu = ReadCvu()  
        self.trigger_dag = trigger_dag      
        self.generate_table = GenerateTable()
        logger.info("CVU inicializado com payload")
    
    def run_workflow(self):
        logger.info("Iniciando workflow para CVU")
        self.run_process()
    
    def run_process(self):
        logger.info("Executando processo para CVU")
        cvus_processados = self.read_cvu.run_workflow()
        
        if cvus_processados:
            logger.info("Acionando DAG 1.18-PROSPEC_UPDATE com conf: {'produto': 'CVU'}")
            self.trigger_dag(dag_id="1.18-PROSPEC_UPDATE", conf={"produto": "CVU"})
            
            logger.info("Gerando tabelas para os CVUs processados: %s", cvus_processados)
            self.generate_table.run_workflow(cvus_processados)
        else:
            logger.info("Nenhum CVU foi processado, pulando DAG e geração de tabelas")
     
     
class ReadCvu(TasksInterface):
    
    def __init__(self):
        self.logger = logger
        self.constants = constants
        self.pt_to_en_month = {
            "janeiro": "January", "fevereiro": "February", "março": "March",
            "abril": "April", "maio": "May", "junho": "June",
            "julho": "July", "agosto": "August", "setembro": "September",
            "outubro": "October", "novembro": "November", "dezembro": "December"
        }
        self.logger.info("ReadCvu inicializado")
    
    def run_workflow(
        self,
        tipos_cvu:list = ['conjuntural', 'estrutural', 'conjuntural_revisado', 'merchant']
    ):
        self.logger.info("Iniciando run_workflow para ReadCvu")
        cvus_processados = self.run_process(tipos_cvu)
        return cvus_processados
      
    def run_process(
        self,
        tipos_cvu:list = ['conjuntural', 'estrutural', 'conjuntural_revisado', 'merchant']
    ):
        self.logger.info("Iniciando verificação e processamento de CVUs")
        
        cvus_to_process = []
        cvus_processados = []
        
        for tipo_cvu in tipos_cvu:
            try:
                self.logger.debug("Verificando tipo de CVU: %s", tipo_cvu)
                
                data_atualizacao_result = self.get_data_atualizacao_cvu(tipo_cvu)
                data_atualizacao_str = data_atualizacao_result['data_atualizacao'].strftime('%Y-%m-%dT%H:%M:%S')
                
                status_result = self.check_cvu_status_processamento(tipo_cvu, data_atualizacao_str)
                
                if status_result.get('status') == 'processando':
                    cvus_to_process.append({
                        'tipo_cvu': tipo_cvu,
                        'data_atualizacao': data_atualizacao_result['data_atualizacao'],
                        'id_check': status_result.get('id')
                    })
                    self.logger.info("CVU %s precisa ser processado", tipo_cvu)
                else:
                    self.logger.info("CVU %s já foi processado", tipo_cvu)
                    
            except Exception as e:
                self.logger.error("Erro ao verificar tipo de CVU %s: %s", tipo_cvu, str(e), exc_info=True)
                continue
        
        for cvu_info in cvus_to_process:
            try:
                tipo_cvu = cvu_info['tipo_cvu']
                self.logger.info("Processando tipo de CVU: %s", tipo_cvu)
                
                df = self.get_cvu_from_csv(tipo_cvu)
                self.post_data(df, tipo_cvu)
                
                self.mark_cvu_as_processed(cvu_info['id_check'])
                
                tipo_cvu_clean = tipo_cvu.replace('_revisado', '')
                if tipo_cvu_clean not in cvus_processados:
                    cvus_processados.append(tipo_cvu_clean)
                
                self.logger.info("Tipo de CVU processado com sucesso: %s", tipo_cvu)
                
            except Exception as e:
                self.logger.error("Falha ao processar tipo de CVU %s: %s", tipo_cvu, str(e), exc_info=True)
                continue
        
        if not cvus_to_process:
            self.logger.info("Nenhum CVU precisava ser processado")
        else:
            self.logger.info("Processamento concluído para %d CVUs", len(cvus_to_process))
        
        return cvus_processados
    
    def get_cvu_from_csv(self, tipo_cvu: str) -> pd.DataFrame:
        self.logger.info("Baixando dados CVU para tipo: %s", tipo_cvu)
        
        if tipo_cvu not in MAPEAMENTO_CVU:
            raise ValueError(f"Tipo de CVU inválido: {tipo_cvu}")
        
        url = MAPEAMENTO_CVU[tipo_cvu]['url']
        
        try:
            res = requests.get(url)
            res.raise_for_status()
            
            df = pd.read_csv(StringIO(res.text), sep=",", na_values=None)
            df = df.replace('-', None)
            
            df.columns = [sanitize_string(col, '_').lower() for col in df.columns]
            
            if df['mes_referencia'].dtype == 'object':
                df = df.loc[df['mes_referencia'].str[0] != '*'].copy()
            
            df.rename(
                columns={'codigo_modelo_preao': 'codigo_modelo_preco'},
                errors='ignore',
                inplace=True,
            )
            
            df = df.astype(MAPEAMENTO_CVU[tipo_cvu]['columns'])
            df = convert_date_columns(df)
            
            data_atualizacao_result = self.get_data_atualizacao_cvu(tipo_cvu)
            df['dt_atualizacao'] = data_atualizacao_result['data_atualizacao'].date()
            
            df.columns = [col.lower() for col in df.columns]
            
            rename_dict = {
                "cvu_cf": "vl_cvu_cf",
                "cvu_scf": "vl_cvu_scf",
                "cvu_conjuntural": "vl_cvu",
                "cvu_estrutural": "vl_cvu",
                "codigo_modelo_preco": "cd_usina"
            }
            df.rename(columns=rename_dict, errors='ignore', inplace=True)
            
            df = df.round(2)
            df.replace({np.nan: None, np.inf: None, -np.inf: None}, inplace=True)
            
            df['tipo_cvu'] = tipo_cvu.replace('_revisado', '')
            df['fonte'] = "CCEE_" + tipo_cvu
            
            if 'ano_horizonte' not in df.columns or df['ano_horizonte'].isnull().all():
                df['ano_horizonte'] = df['mes_referencia'].astype(str).str[:4]
            
            self.logger.info("Dados CVU processados com sucesso para tipo: %s, linhas: %d", tipo_cvu, len(df))
            return df
            
        except Exception as e:
            self.logger.error("Falha ao baixar/processar dados CVU para tipo %s: %s", tipo_cvu, str(e))
            raise
    
    def get_data_atualizacao_cvu(self, tipo_cvu: str) -> dict:
        self.logger.info("Obtendo data de atualização para tipo CVU: %s", tipo_cvu)
        
        search_url = f"https://dadosabertos.ccee.org.br/dataset/custo_variavel_unitario_{tipo_cvu}"
        
        try:
            response = requests.get(search_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            section = soup.find("section", class_="additional-info")
            
            if not section:
                raise ValueError("Seção 'additional-info' não encontrada na página")
            
            tabela = section.find("tbody")
            if not tabela:
                raise ValueError("Nenhuma tabela encontrada com class 'additional-info'")
            
            linhas_tabela = tabela.find_all("th")
            for i, linha in enumerate(linhas_tabela):
                if "atualização" in linha.text.lower():
                    date_str = tabela.find_all("tr")[i].find("span").text.strip()
                    
                    for pt_month, en_month in self.pt_to_en_month.items():
                        if date_str.lower().startswith(pt_month):
                            date_str = date_str.replace(pt_month, en_month, 1)
                            break
                    
                    date_atualizacao = datetime.datetime.strptime(date_str, "%B %d, %Y, %H:%M (BRT)")
                    self.logger.info("Data de atualização encontrada para %s: %s", tipo_cvu, date_atualizacao)
                    return {"tipo_cvu": tipo_cvu, "data_atualizacao": date_atualizacao}
            
            raise ValueError("Data de atualização não encontrada na tabela")
            
        except Exception as e:
            self.logger.error("Falha ao obter data de atualização para tipo CVU %s: %s", tipo_cvu, str(e))
            raise
    
    def check_cvu_status_processamento(self, tipo_cvu: str, data_atualizacao: str):
        print()
        self.logger.info("Verificando status de processamento para %s", tipo_cvu)
        try:
            res = requests.get(
                f"{self.constants.BASE_URL}/api/v2/decks/check-cvu",
                params={'tipo_cvu': tipo_cvu, 'data_atualizacao': data_atualizacao},
                headers=get_auth_header()
            )
            
            if res.status_code == 404:
                # Criar novo registro de status
                self.logger.info("Criando novo registro de status para %s", tipo_cvu)
                res = requests.post(
                    f"{self.constants.BASE_URL}/api/v2/decks/check-cvu",
                    json={'tipo_cvu': tipo_cvu, 'data_atualizacao': data_atualizacao, 'status': 'processando'},
                    headers=get_auth_header()
                )
                return res.json()
            
            return res.json()
        except Exception as e:
            self.logger.error("Erro ao verificar status de processamento: %s", str(e))
            raise

    def mark_cvu_as_processed(self, id_check_cvu: int):
        self.logger.info("Marcando CVU como processado: %s", id_check_cvu)
        try:
            res = requests.patch(
                f"{self.constants.BASE_URL}/api/v2/decks/check-cvu/{id_check_cvu}/status",
                params={'status': 'processado'},
                headers=get_auth_header()
            )
            return res.json()
        except Exception as e:
            self.logger.error("Erro ao marcar CVU como processado: %s", str(e))
            raise

    def post_data(self, data_in: pd.DataFrame, tipo_cvu: str) -> dict:
        self.logger.info("Enviando dados CVU para banco de dados para tipo: %s, linhas: %d", tipo_cvu, len(data_in))
        try:
            url = f'{self.constants.BASE_URL}/api/v2/decks/cvu'
            if tipo_cvu == 'merchant':
                url += '/merchant'
            
            for col in data_in.columns:
                if data_in[col].dtype == 'object':
                    data_in[col] = data_in[col].apply(lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else x)
            
            res = requests.post(
                url,
                json=data_in.to_dict('records'),
                headers=get_auth_header()
            )
            
            if res.status_code >= 300:
                self.logger.error("Falha ao enviar dados CVU: status %d, resposta: %s",
                                res.status_code, res.text)
                raise Exception(f"Erro ao postar CVU: {res.status_code} - {res.text}")
            
            self.logger.info("Dados CVU enviados com sucesso, status da resposta: %d", res.status_code)
            return res.json()
        
        except Exception as e:
            self.logger.error("Falha ao enviar dados CVU: %s", str(e), exc_info=True)
            raise
    
    
class GenerateTable(TasksInterface):
    
    def __init__(self):
        self.logger = logger
        self.logger.info("GenerateTable inicializado")
    
    def run_workflow(self, tipo_cvu=None):
        self.logger.info("Iniciando run_workflow para GenerateTable")
        self.run_process(tipo_cvu)

    def run_process(self, tipo_cvu):
        self.logger.info("Gerando tabela para tipos de CVU: %s", tipo_cvu)
        df_dt = self.get_datas_atualizacao()
        for cvu in tipo_cvu:
            self.logger.debug("Processando tipo de CVU: %s", cvu)
            self.generate_table(df_dt[df_dt['tipo_cvu']==cvu])
        self.logger.info("Geração de tabela concluída")
    
    
    def get_datas_atualizacao(self) -> pd.DataFrame:
        self.logger.info("Recuperando dados históricos de CVU")
        try:
            df_hist = pd.DataFrame(self.get_data(constants.GET_HISTORICO_CVU,'')['data'])
            self.logger.debug("Dados históricos de CVU recuperados, linhas: %d", len(df_hist))
            df_hist['tipo_cvu'] = df_hist['tipo_cvu'].replace('conjuntural_revisado', 'conjuntural')
            self.logger.debug("Substituído 'conjuntural_revisado' por 'conjuntural'")
            df_hist["data_atualizacao"] = pd.to_datetime(df_hist["data_atualizacao"])
            self.logger.debug("Convertido data_atualizacao para datetime")
            df_hist = df_hist.sort_values(["tipo_cvu", "data_atualizacao"], ascending=[True, False])
            self.logger.debug("Dados ordenados por tipo_cvu e data_atualizacao")
            return df_hist.groupby("tipo_cvu").head(2).reset_index(drop=True)
        except (KeyError, ValueError) as e:
            self.logger.error("Falha ao processar dados históricos de CVU: %s", str(e))
            raise
    
        
    def generate_table(self, df_dt):
        self.logger.info("Gerando tabela para dados CVU")
     
        date = pd.to_datetime(df_dt['data_atualizacao'].values[0]).strftime('%Y-%m-%d')
        tipo_cvu = df_dt['tipo_cvu'].values[0]
        self.logger.debug("Processando dados CVU para data: %s, tipo: %s", date, tipo_cvu)
        df_atu = pd.DataFrame(self.get_data(constants.GET_CVU,{'dt_atualizacao':date, 'fonte':tipo_cvu}))
        if df_atu.empty:
            self.logger.warning("Nenhum dado encontrado para %s, tentando versão revisada", tipo_cvu)
            tipo_cvu = tipo_cvu + '_revisado'
            df_atu = pd.DataFrame(self.get_data(constants.GET_CVU,{'dt_atualizacao':date, 'fonte':tipo_cvu}))
            self.logger.debug("Dados recuperados para tipo CVU revisado: %s", tipo_cvu)
            
        df_atu = df_atu[df_atu['mes_referencia']==max(df_atu['mes_referencia'])]
        df_atu = df_atu.sort_values('cd_usina').reset_index(drop=True)
        
        date = pd.to_datetime(df_dt['data_atualizacao'].values[1]).strftime('%Y-%m-%d')
        tipo_cvu = df_dt['tipo_cvu'].values[1]
        self.logger.debug("Processando dados CVU anteriores para data: %s, tipo: %s", date, tipo_cvu)
        df_ant = pd.DataFrame(self.get_data(constants.GET_CVU,{'dt_atualizacao':date, 'fonte':tipo_cvu}))
        if df_ant.empty:
            self.logger.warning("Nenhum dado encontrado para %s, tentando versão revisada", tipo_cvu)
            tipo_cvu = tipo_cvu + '_revisado'
            df_ant = pd.DataFrame(self.get_data(constants.GET_CVU,{'dt_atualizacao':date, 'fonte':tipo_cvu}))
            self.logger.debug("Dados recuperados para tipo CVU revisado: %s", tipo_cvu)
        
        df_nome = pd.DataFrame(self.get_data(constants.GET_NOME_UTE,{'dt_atualizacao':'', 'fonte':''}))
        df_nome = df_nome.rename(columns={'sigla_parcela':'NOME'})
        df_nome['NOME'] = df_nome['NOME'].str.replace('UTE ','')
        tipo_cvu = tipo_cvu.replace('_revisado', '')   
        df_ant = df_ant[df_ant['mes_referencia']==max(df_ant['mes_referencia'])]
        df_ant = df_ant.sort_values('cd_usina').reset_index(drop=True)
        df_ant = df_ant[df_ant['cd_usina'].isin(df_atu['cd_usina'])].reset_index(drop=True)
        ant_pivot = df_ant.pivot_table(index='cd_usina', columns='ano_horizonte', values='vl_cvu')
        atu_pivot = df_atu.pivot_table(index='cd_usina', columns='ano_horizonte', values='vl_cvu')
        ant_pivot.columns = [f'{col}_old' for col in ant_pivot.columns]
        atu_pivot.columns = [f'{col}_new' for col in atu_pivot.columns]

        df_merged = pd.concat([ant_pivot, atu_pivot], axis=1)
        self.logger.debug("Merged ant_pivot e atu_pivot")
        for col in atu_pivot.columns:
            ano = col.split('_')[0]
            col_ant = f'{ano}_old'
            col_atu = f'{ano}_new'
            df_merged[f'{ano}_dif'] = (df_merged[col_atu] - df_merged[col_ant])
            self.logger.debug("Calculada diferença para ano: %s", ano)
              
        cols_dif = [col for col in df_merged.columns if col.endswith('_dif')]
        df_filtrado = df_merged[(df_merged[cols_dif] != 0).any(axis=1)]
        df_merged = df_filtrado.reset_index()
        df_merged = pd.merge(df_merged, df_nome, on='cd_usina', how='left') 
        df_merged = df_merged.sort_values(df_merged.filter(like='_new').columns[0]).reset_index(drop=True)
        df_merged = df_merged.set_index("cd_usina")
        df_merged.columns.name = 'USINA' 
        df_merged.index.name = None 
        
        colunas = ['NOME'] + [col for col in df_merged.columns if col != 'NOME']
        df_merged = df_merged[colunas]     
        df_merged_html = df_merged.style.format(na_rep='', precision=0)     
        df_merged_html.set_caption(f"ATUALIZAÇÃO DE CVU {tipo_cvu.upper()} ")
        self.logger.debug("Definido caption da tabela para tipo CVU: %s", tipo_cvu.upper())

        css = '<style type="text/css">'
        css += 'caption {background-color: #E0E0E0; color: black;}'
        css += 'th {background-color: #E0E0E0; color: black; min-width: 80px;}'
        css += 'td {min-width: 80px;}'
        css += 'table {text-align: center; border-collapse: collapse; border 2px solid black !important}'
        css += '</style>'

        html = df_merged_html.to_html() 
        self.logger.debug("Gerado HTML para tabela")
        html = html.replace('<style type="text/css">\n</style>\n', css)
        self.logger.debug("Aplicado CSS customizado ao HTML")
        image_binary = html_to_image(html)
        self.logger.debug("Convertido HTML para imagem")
        
        send_whatsapp_message(constants.WHATSAPP_DECKS, f'REVISÃO DE CVU {tipo_cvu.upper()}', image_binary)
        self.logger.info("Enviada mensagem WhatsApp com tabela CVU para tipo: %s", tipo_cvu.upper())

    def get_data(self, url, date) -> dict:
        self.logger.info("Recuperando dados do banco de dados com parâmetros: %s", date)
        try:
            res = requests.get(url, params=date, headers=get_auth_header())
            if res.status_code != 200:
                self.logger.error("Falha ao obter dados do banco de dados: status %d, resposta: %s",
                                res.status_code, res.text)
                res.raise_for_status()          
            self.logger.info("Dados recuperados com sucesso do banco de dados, status da resposta: %d", res.status_code)
            return res.json()
        except Exception as e:
            self.logger.error("Falha ao obter dados do banco de dados: %s", str(e), exc_info=True)
            raise    

if __name__ == '__main__':
    t = Cvu()
    t.run_workflow()
    # logger.info("Iniciando execução do script CVU")
    # try:
        # carga = GenerateTable()
        # carga.run_workflow(['conjuntural','estrutural'])
        # logger.info("Execução do script concluída com sucesso")
    # except Exception as e:
        # logger.error("Falha na execução do script: %s", str(e), exc_info=True)
        # raise