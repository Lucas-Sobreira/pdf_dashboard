import os
import camelot
import pandas as pd
import logging
from unidecode import unidecode
from configs.rules.notas import rules_dict
from configs.tools.postgre import RDSPostgreSQLManager

logging.basicConfig(level=logging.INFO)

class PDFTableExtractor:
    def __init__(self, file_name, configs):
        self.path = os.path.abspath(f"files/pdf/{configs["name"].lower()}/{file_name}.pdf")
        self.csv_path = os.path.abspath(f"files/csv/")
        self.file_name = file_name
        self.configs = configs

    def start(self):
        """
        Realiza todas as etapas do processo até a entrega dos dados no BD.
        """

        logging.info(f"Start pdf - {self.file_name}")
        header = self.get_table_data(self.configs["header_table_areas"], self.configs["header_columns"],self.configs["header_fix"])
        main = self.get_table_data(self.configs["table_areas"], self.configs["columns"],self.configs["fix"])
        small = self.get_table_data(self.configs["small_table_areas"], self.configs["small_columns"],self.configs["small_fix"])

        main = self.add_infos(header,main)
        small = self.add_infos(header, small)

        main = self.sanitize_column_names(main)
        if self.configs["small_sanitize"]:
            small = self.sanitize_column_names(small)

        logging.info(f"Saving csv - {self.file_name}")
        self.save_csv(main, self.file_name)
        self.save_csv(small, f"{self.file_name}_small")

        logging.info(f"Sending to DB - {self.file_name}")
        self.send_to_db(main, f"Fatura_{self.configs['name']}".lower())
        self.send_to_db(small, f"Fatura_{self.configs['name']}_small".lower())

        return {"main": main, "small": small}


    def get_table_data(self, table_areas, table_columns, fix = True):
        """
        Baseado nas configurações coleta as informações da tabela principal em cada uma das páginas do PDF. 
        Caso exista apenas 1 página, ele irá pegar as informações apenas desta página. 
        """

        tables = camelot.read_pdf(
            self.path,
            flavor=self.configs["flavor"],
            table_areas=table_areas,
            columns=table_columns,
            strip_text=self.configs["strip_text"],
            pages=self.configs["pages"],
            password=self.configs["password"],
        )

        table_content = [self.fix_header(page.df) if fix else page.df for page in tables]

        result = pd.concat(table_content, ignore_index=True) if len(table_content) > 1 else table_content[0]
        return result

    def save_csv(self, df, file_name):
        """
        Faz a checagem da pasta, caso exista, salva os dados em um arquivo CSV. 
        Caso não exista a pasta, criará utilizando a função 'mkdirs'
        """

        if not os.path.exists(self.csv_path):
            os.makedirs(self.local_path, exist_ok=True)
        path = os.path.join(self.csv_path, f"{file_name}.csv")
        df.to_csv(path, sep=";", index=False)

    def add_infos(self, header, content):
        """
        Coleta as informações que não estão presentes no Dataframe do PDF, mas são necessárias para análises financeiras. 
        Essas mesmas são replicadas conforme o tamanho do Dataframe. Exemplo: Data do pregão, número da nota, posição, ajuste, etc...
        """

        infos = header.iloc[0]
        df = pd.DataFrame([infos.values] * len(content), columns=header.columns)
        content = pd.concat([content.reset_index(drop=True),df.reset_index(drop=True)], axis=1)
        content["Data de Inserção"] = pd.Timestamp('today').normalize()
        return content

    @staticmethod
    def fix_header(df):
        """
        O content do PDF é coletado de tal forma que a primeira linha do Dataframe são as colunas. 
        Tendo isso em vista, as colunas do Dataframe são renomeadas de acordo e realizado um drop na primeira coluna do Dataframe.
        """

        df.columns = df.iloc[0]
        df = df.drop(0)
        df = df.drop(df.columns[0], axis=1)
        return df

    def sanitize_column_names(self, df):
        """
        Nessa função são tratadas algumas particularidades. Como acentuação de palavras, 
        espaços entre as colunas, caracteres alphanumericos e transforma o nome das colunas em minúsculas
        """

        df.columns = df.columns.map(lambda x: unidecode(x))
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace(r'\W', '', regex=True)
        df.columns = df.columns.str.lower()
        return df

    @staticmethod
    def send_to_db(df, table_name):
        """
        Através da classe do Postgres, desenvolvida na pasta ./configs/tools/postgres. 
        É realizada uma conexão com o Banco de Dados (BD) e os dados são salvos.
        """

        try:
            connection = RDSPostgreSQLManager().alchemy()
            df.to_sql(table_name, connection, if_exists="append", index=False)
            logging.info(f"Success to save into {table_name}")
        except Exception as e:
            logging.error(e)

def list_files(folder):
    """
    Lista todos os arquivos no diretório passado.
    """
    
    try:
        files = [os.path.splitext(f)[0] for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        return files
    except FileNotFoundError:
        logging.info(f"A pasta '{folder}' não foi encontrada.")
        return []
    except Exception as e:
        logging.info(f"Ocorreu um erro: {e}")
        return []

if __name__ == "__main__":
    corretora = 'redrex'
    path = os.path.abspath(f"files/pdf/{corretora}/")
    files = list_files(path)
    
    for file in files:
        extractor = PDFTableExtractor(file, configs=rules_dict[corretora]).start()
    logging.info("Todos os arquivos foram processados")