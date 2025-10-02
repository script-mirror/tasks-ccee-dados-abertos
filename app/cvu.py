
import sys
import requests
import pandas as pd
from middle.utils import html_to_image
from middle.message import send_whatsapp_message
from middle.utils import setup_logger, Constants, get_auth_header
from middle.airflow import trigger_dag

logger = setup_logger()
constants = Constants()

class Cvu:
    
    def __init__(self):
        self.read_cvu = ReadCvu()  
        self.trigger_dag = trigger_dag      
        self.generate_table = GenerateTable()
        logger.info("Initialized Cvu with payload")
    
    def run_workflow(self):
        logger.info("Starting workflow for Cvu")
        self.run_process()
   
    def run_process(self):
        logger.info("Running process for Cvu")
        self.read_cvu.run_workflow()
        logger.debug("Triggering DAG 1.18-PROSPEC_UPDATE with conf: {'produto': 'CVU'}")
        self.trigger_dag(dag_id="1.18-PROSPEC_UPDATE", conf={"produto": "CVU"})
        tipo_cvu = ['conjuntural','estrutural']
        self.generate_table.run_workflow(tipo_cvu)
     
     
class ReadCvu:
    
    def __init__(self):
        self.logger = logger
        self.logger.info("Initialized ReadCvu")
    
    def run_workflow(self):
        self.logger.info("Starting run_workflow for ReadCvu")
        self.logger.debug("Calling run_process in run_workflow")
        self.run_process(None)
      
    def run_process(self, base_path):
        self.logger.info("Processing load data from base path: %s", base_path)
        self.logger.debug("Calling read_file with base_path: %s", base_path)
        self.read_file(base_path)
      
    def read_file(self, base_path):
        self.logger.info("Reading week load data from base path: %s", base_path)
        self.logger.debug("Completed reading data from base path: %s", base_path)
    
    def post_data(self, data_in: pd.DataFrame) -> dict:
        self.logger.info("Posting load data to database, rows: %d", len(data_in))
        try:
            res = requests.post(
                constants.POST_DECOMP_CARGA_DECOMP,
                json=data_in.to_dict('records'),
                headers=get_auth_header()
            )
            if res.status_code != 200:
                self.logger.error("Failed to post data to database: status %d, response: %s",
                                res.status_code, res.text)
                res.raise_for_status()
            
            self.logger.info("Successfully posted data to database, response status: %d", res.status_code)
            return pd.DataFrame(res.json())
        
        except Exception as e:
            self.logger.error("Failed to post data to database: %s", str(e), exc_info=True)
            raise
    
    
class GenerateTable:
    
    def __init__(self):
        self.logger = logger
        self.logger.info("Initialized GenerateTable")
    
    def run_workflow(self, tipo_cvu=None):
        self.logger.info("Starting run_workflow for GenerateTable")
        self.run_process(tipo_cvu)

    def run_process(self, tipo_cvu):
        self.logger.info("Generating table for CVU types: %s", tipo_cvu)
        df_dt = self.get_datas_atualizacao()
        for cvu in tipo_cvu:
            self.logger.debug("Processing CVU type: %s", cvu)
            self.generate_table(df_dt[df_dt['tipo_cvu']==cvu])
        self.logger.info("Table generation completed")
    
    
    def get_datas_atualizacao(self) -> pd.DataFrame:
        self.logger.info("Retrieving historical CVU data")
        try:
            df_hist = pd.DataFrame(self.get_data(constants.GET_HISTORICO_CVU,'')['data'])
            self.logger.debug("Retrieved historical CVU data, rows: %d", len(df_hist))
            df_hist['tipo_cvu'] = df_hist['tipo_cvu'].replace('conjuntural_revisado', 'conjuntural')
            self.logger.debug("Replaced 'conjuntural_revisado' with 'conjuntural'")
            df_hist["data_atualizacao"] = pd.to_datetime(df_hist["data_atualizacao"])
            self.logger.debug("Converted data_atualizacao to datetime")
            df_hist = df_hist.sort_values(["tipo_cvu", "data_atualizacao"], ascending=[True, False])
            self.logger.debug("Sorted data by tipo_cvu and data_atualizacao")
            return df_hist.groupby("tipo_cvu").head(2).reset_index(drop=True)
        except (KeyError, ValueError) as e:
            self.logger.error("Failed to process historical CVU data: %s", str(e))
            raise
    
        
    def generate_table(self, df_dt):
        self.logger.info("Generating table for CVU data")
     
        date = pd.to_datetime(df_dt['data_atualizacao'].values[0]).strftime('%Y-%m-%d')
        tipo_cvu = df_dt['tipo_cvu'].values[0]
        self.logger.debug("Processing CVU data for date: %s, type: %s", date, tipo_cvu)
        df_atu = pd.DataFrame(self.get_data(constants.GET_CVU,{'dt_atualizacao':date, 'fonte':tipo_cvu}))
        if df_atu.empty:
            self.logger.warning("No data found for %s, trying revised version", tipo_cvu)
            tipo_cvu = tipo_cvu + '_revisado'
            df_atu = pd.DataFrame(self.get_data(constants.GET_CVU,{'dt_atualizacao':date, 'fonte':tipo_cvu}))
            self.logger.debug("Retrieved data for revised CVU type: %s", tipo_cvu)
            
        df_atu = df_atu[df_atu['mes_referencia']==max(df_atu['mes_referencia'])]
        df_atu = df_atu.sort_values('cd_usina').reset_index(drop=True)
        
        date = pd.to_datetime(df_dt['data_atualizacao'].values[1]).strftime('%Y-%m-%d')
        tipo_cvu = df_dt['tipo_cvu'].values[1]
        self.logger.debug("Processing previous CVU data for date: %s, type: %s", date, tipo_cvu)
        df_ant = pd.DataFrame(self.get_data(constants.GET_CVU,{'dt_atualizacao':date, 'fonte':tipo_cvu}))
        if df_ant.empty:
            self.logger.warning("No data found for %s, trying revised version", tipo_cvu)
            tipo_cvu = tipo_cvu + '_revisado'
            df_ant = pd.DataFrame(self.get_data(constants.GET_CVU,{'dt_atualizacao':date, 'fonte':tipo_cvu}))
            self.logger.debug("Retrieved data for revised CVU type: %s", tipo_cvu)
        
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
        self.logger.debug("Merged ant_pivot and atu_pivot")
        for col in atu_pivot.columns:
            ano = col.split('_')[0]
            col_ant = f'{ano}_old'
            col_atu = f'{ano}_new'
            df_merged[f'{ano}_dif'] = (df_merged[col_atu] - df_merged[col_ant])
            self.logger.debug("Calculated difference for year: %s", ano)
              
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
        self.logger.debug("Set table caption for CVU type: %s", tipo_cvu.upper())

        css = '<style type="text/css">'
        css += 'caption {background-color: #E0E0E0; color: black;}'
        css += 'th {background-color: #E0E0E0; color: black; min-width: 80px;}'
        css += 'td {min-width: 80px;}'
        css += 'table {text-align: center; border-collapse: collapse; border 2px solid black !important}'
        css += '</style>'

        html = df_merged_html.to_html() 
        self.logger.debug("Generated HTML for tables")
        html = html.replace('<style type="text/css">\n</style>\n', css)
        self.logger.debug("Applied custom CSS to HTML")
        image_binary = html_to_image(html)
        self.logger.debug("Converted HTML to image")
        
        send_whatsapp_message(constants.WHATSAPP_DECKS, f'REVISÃO DE CVU {tipo_cvu.upper()}', image_binary)
        self.logger.info("Sent WhatsApp message with CVU table for type: %s", tipo_cvu.upper())

    def get_data(self, url, date) -> dict:
        self.logger.info("Retrieving data from database with params: %s", date)
        try:
            res = requests.get(url, params=date, headers=get_auth_header())
            if res.status_code != 200:
                self.logger.error("Failed to get data from database: status %d, response: %s",
                                res.status_code, res.text)
                res.raise_for_status()          
            self.logger.info("Successfully retrieved data from database, response status: %d", res.status_code)
            return res.json()
        except Exception as e:
            self.logger.error("Failed to get data from database: %s", str(e), exc_info=True)
            raise    

if __name__ == '__main__':
    logger.info("Starting Cvu script execution")
    try:
        carga = GenerateTable()
        carga.run_workflow(['conjuntural','estrutural'])
        logger.info("Script execution completed successfully")
    except Exception as e:
        logger.error("Script execution failed: %s", str(e), exc_info=True)
        raise