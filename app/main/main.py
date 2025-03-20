import shutil
import requests
from datetime import datetime
import os
import pandas as pd
import time
from decimal import Decimal
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
# Corrected import statement: Assuming 'schemas.py' is in the same directory
from userschema import Company, Establishment, Simples, Partner, LegalNature, City, Country, Cnae, PartnerQualification
from bd import getSession # Assuming bd.py is also in the same directory

def download_with_retries(url, zip_file_path, max_retries=10, backoff_factor=2):
    attempt = 0
    while attempt < max_retries:
        try:
            print(f"Baixando arquivo (tentativa {attempt + 1}/{max_retries})...")
            r = requests.get(url, stream=True)
            r.raise_for_status()

            with open(zip_file_path, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=128):
                    fd.write(chunk)

            print(f"Download concluído com sucesso: {zip_file_path}")
            return

        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar o arquivo: {e}")
            attempt += 1
            if attempt < max_retries:
                wait_time = backoff_factor * attempt
                print(f"Tentando novamente em {wait_time} segundos...")
                time.sleep(wait_time)
            else:
                print("Número máximo de tentativas atingido. Falha no download.")
                raise


def parse_date(date_str):
        """Função para converter uma string no formato YYYYMMDD em um objeto datetime"""
        try:
            # Tenta converter a string no formato correto
            return datetime.strptime(date_str, '%Y%m%d')
        except (ValueError, TypeError):
            # Se houver erro (como uma data inválida), retorna None
            return None

def upsertCSVIntoBD(file_name, corrected_csv_file):
    """
    Lê o arquivo CSV corrigido e insere os dados no banco de dados.

    :param file_name: Nome do arquivo (para identificar o tipo de dados).
    :param corrected_csv_file: Caminho para o arquivo CSV corrigido.
    """
    # Conectar ao banco de dados
    date = datetime.now()
    session = getSession()
    inserted_count = 0  # Contador de registros inseridos

     # Verifica se o campo existe e não é nulo, caso contrário, atribui um valor padrão
    def safe_get(row, column, default_value=""):
          return str(row.get(column, default_value)).strip() if row.get(column) else default_value

    try:
        # Ler o CSV corrigido
        dtype = {
          'DDD 1': 'Int64',  # Tipo de dado adequado
          'DDD 2': 'Int64',
          'TELEFONE 1': 'str',
          'TELEFONE 2': 'str', # Usar string para garantir que não ocorram problemas com números com espaços ou caracteres não numéricos
          'FAX': 'str'
          # Continue definindo os tipos conforme necessário
        }

        df = pd.read_csv(corrected_csv_file, encoding='utf-8', dtype=dtype, low_memory=False)
        print(f"Total de linhas no CSV: {len(df)}")

        if 'DDD 1' in df.columns:
            df['DDD 1'] = df['DDD 1'].fillna(0).astype(float).astype(int).astype(str).str.replace(' ', '')
        if 'DDD 2' in df.columns:
            df['DDD 2'] = df['DDD 2'].fillna(0).astype(float).astype(int).astype(str).str.replace(' ', '')
        if 'TELEFONE 1' in df.columns:
            df['TELEFONE 1'] = df['TELEFONE 1'].fillna('0').astype(str).str.replace(r'\D', '')  # Remove qualquer caractere não numérico
            df['TELEFONE 1'] = df['TELEFONE 1'].fillna('0').astype(str).str.replace(" ", '')
            df['TELEFONE 1'] = df['TELEFONE 1'].apply(lambda x: str(int(float(x))) if x != '0' else '0')  # Converte para número
        if 'TELEFONE 2' in df.columns:
            df['TELEFONE 2'] = df['TELEFONE 2'].fillna('0').astype(str).str.replace(r'\D', '')  # Remove qualquer caractere não numérico
            df['TELEFONE 2'] = df['TELEFONE 2'].fillna('0').astype(str).str.replace(" ", '')
            df['TELEFONE 2'] = df['TELEFONE 2'].apply(lambda x: str(int(float(x))) if x != '0' else '0')  # Converte para número
        if 'FAX' in df.columns:
            df['FAX'] = df['FAX'].fillna(0).astype(str).str.replace(" ","").apply(lambda x: str(int(float(x))) if x != '0' else '0')
        if 'CEP' in df.columns:
            df['CEP'] = df['CEP'].fillna(0).astype(float).astype(int).astype(str).str.replace(' ', '').replace("-","")

        if 'PAIS' in df.columns:
            df['PAIS'] = df['PAIS'].fillna(0).apply(lambda x: int(float(x)))
        if 'CORREIO ELETRONICO' in df.columns:
            df['CORREIO ELETRONICO'] = df['CORREIO ELETRONICO'].fillna('')  # Substitui NaN por uma string vazia

        # if 'CAPITAL SOCIAL DA EMPRESA' in df.columns:
        #     df['CAPITAL SOCIAL DA EMPRESA'] = df['CAPITAL SOCIAL DA EMPRESA'].apply(lambda x: str(x).replace(" ", "") if isinstance(x, str) else x)
        #     df['CAPITAL SOCIAL DA EMPRESA'] = pd.to_numeric(df['CAPITAL SOCIAL DA EMPRESA'], errors='coerce')


        # Processar dados de data, tratando valores NaN com None
        if 'DATA DE INICIO DE ATIVIDADE' in df.columns:
            df['DATA DE INICIO DE ATIVIDADE'] = pd.to_datetime(df['DATA DE INICIO DE ATIVIDADE'], format='%Y%m%d', errors='coerce')
        if 'DATA DA SITUACAO ESPECIAL' in df.columns:
            df['DATA DA SITUACAO ESPECIAL'] = pd.to_datetime(df['DATA DA SITUACAO ESPECIAL'], format='%Y%m%d', errors='coerce')

        if 'CNPN BASICO' in df.columns:
            df['CNPJ BASICO'] = df['CNPJ BASICO'].astype(str).str.replace(' ', '')

        for index, row in df.iterrows():
            if 'Empresas' in file_name:
                company = Company(
                    base_cnpj='1234567',
                      social_reason_business_name='ata',
                      legal_nature='1015',
                      responsible_qualification='10',
                      social_capital_company=1000.50,
                      company_size='1',
                      responsible_federative_entity='teste',
                      updated_at=date
                )

              #   company = Company(
              #      base_cnpj=str(row['CNPJ BASICO']),
              #       social_reason_business_name=row['RAZAO SOCIAL/NOME EMPRESARIAL'],
              #       legal_nature=row['NATUREZA JURIDICA'],
              #       responsible_qualification=row['QUALIFICACAO DO RESPONSAVEL'],
              #       social_capital_company=row['CAPITAL SOCIAL DA EMPRESA'].replace(',', '.'),
              #       company_size=row['PORTE DA EMPRESA'],
              #       responsible_federative_entity=row['ENTE FEDERATIVO RESPONSÁVEL'],
              #       updated_at=date
              # )
                print(f'empresa: \n{company}')
                session.merge(company)  # 'merge' faz o upsert
                inserted_count += 1  # Incrementa o contador

            elif 'Estabelecimento' in file_name:
              establishment = Establishment(
                  base_cnpj=safe_get(row, 'CNPJ BASICO').replace(" ",""),
                  cnpj_dv=safe_get(row, 'CNPJ DV'),
                  cnpj_order=safe_get(row, 'CNPJ ORDEM'),
                  fantasy_name=safe_get(row, 'NOME FANTASIA'),
                  identifier_branch_matriz=safe_get(row, 'IDENTIFICADOR MATRIZ/FILIAL', ''),
                  cadastral_situation=safe_get(row, 'SITUACAO CADASTRAL', ''),
                  cadastral_situation_reason=safe_get(row, 'MOTIVO SITUACAO CADASTRAL', ''),
                  city_name_exterior=safe_get(row, 'NOME DA CIDADE NO EXTERIOR', ''),
                  country=safe_get(row, 'PAIS', '105'),
                  activity_start_date=parse_date(safe_get(row, 'DATA DE INICIO DE ATIVIDADE', '1900-01-01')),  # Valida a data ou define um padrão
                  special_situation_date=parse_date(safe_get(row, 'DATA DA SITUACAO ESPECIAL', '1900-01-01')),
                  cnae_main=safe_get(row, 'CNAE FISCAL PRINCIPAL', ''),
                  street_type=safe_get(row, 'TIPO DE LOGRADOURO', ''),
                  street=safe_get(row, 'LOGRADOURO', ''),
                  number=safe_get(row, 'NUMERO', ''),
                  complement=safe_get(row, 'COMPLEMENTO', ''),
                  neighborhood=safe_get(row, 'BAIRRO', ''),
                  cep=safe_get(row, 'CEP', ''),
                  cnpj=f"{safe_get(row, 'CNPJ BASICO')}000{safe_get(row, 'CNPJ ORDEM')}{safe_get(row, 'CNPJ DV')}",  # Concatenação de CNPJ
                  city=safe_get(row, 'MUNICIPIO', ''),
                  ddd_1=safe_get(row, 'DDD 1', ''),
                  phone_1= safe_get(row, 'TELEFONE 1', '').replace(" ", ""),
                  ddd_2=safe_get(row, 'DDD 2', ''),
                  phone_2=safe_get(row, 'TELEFONE 2', '').replace(" ",""),
                  fax_ddd=safe_get(row, 'DDD DO FAX', ''),
                  fax=safe_get(row, 'FAX', '').replace(" ", ""),
                  electronic_mail=safe_get(row, 'CORREIO ELETRONICO', ''),
                  special_situation=safe_get(row, 'SITUACAO ESPECIAL', ''),
                  uf=safe_get(row, 'UF', ''),
                  updated_at=datetime.now()  # Data e hora atuais
              )



              session.merge(establishment)
              inserted_count += 1

            elif 'Socios' in file_name:
                try:
                    date_entry = datetime.strptime(str(row['DATA DE ENTRADA SOCIEDADE']), '%Y%m%d') if pd.notna(row['DATA DE ENTRADA SOCIEDADE']) else None
                except ValueError:
                    date_entry = None

                partner = Partner(
                    base_cnpj=row['CNPJ BASICO'],
                    partner_identifier=row['IDENTIFICADOR DE SOCIO'],
                    partner_name_social_reason=row['NOME DO SOCIO/RAZAO SOCIAL'],
                    partner_cpf_cnpj=row['CNPJ/CPF DO SOCIO'],
                    partner_qualification=row['QUALIFICACAO DO SOCIO'],
                    date_entry_society=date_entry,
                    country=row['PAIS'],
                    cpf_legal_representative=row['REPRESENTANTE LEGAL'],
                    representative_name=row['NOME DO REPRESENTANTE'],
                    legal_representative_qualification=row['QUALIFICACAO DO REPRESENTANTE LEGAL'],
                    age_group=row['FAIXA ETARIA'],
                    updated_at=date
                )
                session.merge(partner)
                inserted_count += 1

            elif 'simples' in file_name.lower():
                simples = Simples(
                    base_cnpj=str(row['CNPJ BASICO']),
                    simples_option=row['OPCAO PELO SIMPLES'],
                    simples_option_date=parse_date(safe_get(row,'DATA DE OPCAO PELO SIMPLES','1900-01-01')),
                    simples_option_exclusion_date=parse_date(safe_get(row,'DATA DE EXCLUSAO DO SIMPLES', '1900-01-01')),
                    mei_option=row['OPCAO PELO MEI'],
                    mei_option_date=parse_date(safe_get(row,'DATA DE OPCAO PELO MEI', '1900-01-01')),
                    mei_exclusion_date=parse_date(safe_get(row,'DATA DE EXCLUSAO DO MEI', '1900-01-01')),
                    updated_at=date

                )
                print(f"Objeto gerado: {simples}")
                session.merge(simples)
                inserted_count += 1

            elif 'Cnaes' in file_name:
                cnae = Cnae(
                    code=str(row['CODIGO']),
                    description=row['DESCRICAO'],
                    updated_at=date
                )
                session.merge(cnae)
                inserted_count += 1

            elif 'Naturezas' in file_name:
                nature = LegalNature(
                    code=str(row['CODIGO']),
                    description=row['DESCRICAO'],
                    updated_at=date
                )
                session.merge(nature)
                inserted_count += 1

            elif 'Qualificacoes' in file_name:
                partnerQuali = PartnerQualification(
                    code=str(row['CODIGO']),
                    description=row['DESCRICAO'],
                    updated_at=date
                )
                session.merge(partnerQuali)
                inserted_count += 1

            elif 'Municipios' in file_name:
                city = City(
                    code=str(row['CODIGO']),
                    description=row['DESCRICAO'],
                    updated_at=date
                )
                session.merge(city)
                inserted_count += 1

            elif 'Paises' in file_name:
                country = Country(
                    code=str(row['CODIGO']),
                    description=row['DESCRICAO'],
                    updated_at=date
                )
                session.merge(country)
                inserted_count += 1
            # if inserted_count >= 1:
            #     session.commit()
            #     break  # Reseta o contador após inserir 100 registros
            # # Verifica se já inseriu 100 registros

        session.commit()
        print(f"Dados do arquivo {file_name} inseridos/atualizados com sucesso no banco de dados.")
        print(f"Total de registros inseridos: {inserted_count}")

    except IntegrityError as e:
        session.rollback()
        print(f"Erro de integridade ao inserir dados do arquivo {file_name}: {e}")
    except Exception as e:
        session.rollback()
        print(f"Erro ao processar o arquivo {file_name}: {e}")


def getFiles(fileName, month, year):
    """
    Baixa e processa os arquivos para inserção no banco de dados, realizando um upsert para cada um.

    :param fileName: Nome do arquivo (para identificar o tipo de dados).
    :param month: Mês do arquivo a ser processado.
    :param year: Ano do arquivo a ser processado.
    """
    csvHeader = []
    now = datetime.now()
    counter = 0  # Iniciando com o arquivo 9 para testar

    main_directory = '/content'

    print(f"Definindo cabeçalhos para o arquivo {fileName}...")

    # Definir os cabeçalhos conforme o tipo de arquivo
    if fileName in ['Cnaes', 'Naturezas', 'Qualificacoes', 'Municipios', 'Paises']:
        csvHeader = ['CODIGO', 'DESCRICAO']
    elif fileName in ['Socios']:
        csvHeader = [
            'CNPJ BASICO', 'IDENTIFICADOR DE SOCIO', 'NOME DO SOCIO/RAZAO SOCIAL', 'CNPJ/CPF DO SOCIO', 'QUALIFICACAO DO SOCIO',
            'DATA DE ENTRADA SOCIEDADE', 'PAIS', 'REPRESENTANTE LEGAL', 'NOME DO REPRESENTANTE', 'QUALIFICACAO DO REPRESENTANTE LEGAL',
            'FAIXA ETARIA'
        ]
    elif fileName in ['Simples']:
        csvHeader = [
            'CNPJ BASICO', 'OPCAO PELO SIMPLES', 'DATA DE OPCAO PELO SIMPLES', 'DATA DE EXCLUSAO DO SIMPLES',
            'OPCAO PELO MEI', 'DATA DE OPCAO PELO MEI', 'DATA DE EXCLUSAO DO MEIO'
        ]
    elif fileName in ['Estabelecimento', 'ESTABELE', 'Estabelecimentos']:
        csvHeader = [
            'CNPJ BASICO', 'CNPJ ORDEM', 'CNPJ DV', 'IDENTIFICADOR MATRIZ/FILIAL', 'NOME FANTASIA', 'SITUACAO CADASTRAL',
            'DATA SITUACAO CADASTRAL', 'MOTIVO SITUACAO CADASTRAL', 'NOME DA CIDADE NO EXTERIOR', 'PAIS', 'DATA DE INICIO DE ATIVIDADE',
            'CNAE FISCAL PRINCIPAL', 'CNAE FISCAL SECUNDARIA', 'TIPO DE LOGRADOURO', 'LOGRADOURO', 'NUMERO', 'COMPLEMENTO', 'BAIRRO',
            'CEP', 'UF', 'MUNICIPIO', 'DDD 1', 'TELEFONE 1', 'DDD 2', 'TELEFONE 2', 'DDD DO FAX', 'FAX', 'CORREIO ELETRONICO',
            'SITUACAO ESPECIAL', 'DATA DA SITUACAO ESPECIAL'
        ]
    elif fileName in ['Empresas']:
      csvHeader = [
        'CNPJ BASICO', 'RAZAO SOCIAL/NOME EMPRESARIAL', 'NATUREZA JURIDICA',
        'QUALIFICACAO DO RESPONSAVEL', 'CAPITAL SOCIAL DA EMPRESA', 'PORTE DA EMPRESA',
        'ENTE FEDERATIVO RESPONSÁVEL'
      ]

    elif fileName in ['Motivos']:
        csvHeader = ['Test', 'Test']

    print(f"Cabeçalhos definidos: {csvHeader}")

    if month is None:
        month = now.month
    if year is None:
        year = now.year

    print(f"Data configurada: {month}/{year}")

    arquivos_folder_path = os.path.join(main_directory, 'arquivos')

    if not os.path.exists(arquivos_folder_path):
        os.makedirs(arquivos_folder_path)
        print(f"Pasta 'arquivos' criada em: {arquivos_folder_path}")

    zip_file_path = os.path.join(arquivos_folder_path, f"{fileName}_{counter}.zip")
    extracted_folder_path = os.path.join(arquivos_folder_path, 'extracted')

    if fileName in ['Empresas', 'Estabelecimentos', 'Socios']:
        while True:
            print(f"Baixando arquivo {fileName}_{counter}.zip...")

            url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year}-0{month}/{fileName}{counter}.zip'
            print(f"URL: {url}")

            try:
                download_with_retries(url, zip_file_path)  # Tenta baixar o arquivo
            except FileNotFoundError:
                print(f"Arquivo {fileName}_{counter}.zip não encontrado, finalizando.")
                break  # Sai do loop quando o arquivo não é encontrado (erro 404)
            except Exception as e:
                print(f"Erro ao baixar o arquivo {fileName}_{counter}.zip: {str(e)}")
                break

            try:
                if not os.path.exists(extracted_folder_path):
                    os.makedirs(extracted_folder_path)
                    print(f"Pasta de extração criada em: {extracted_folder_path}")

                shutil.unpack_archive(zip_file_path, extracted_folder_path)
                print(f"Arquivo {zip_file_path} extraído com sucesso.")

            except Exception as e:
                print(f"Erro ao extrair o arquivo: {e}")
                return

            extracted_files = os.listdir(extracted_folder_path)
            print(f"Arquivos extraídos: {extracted_files}")
            csv_file = None
            for file in extracted_files:
        # Check if the current item is a file and not a directory before processing
              file_path = os.path.join(extracted_folder_path, file)
              if os.path.isfile(file_path):  # Added file type check
                csv_file = file_path
                break  # Stop searching once a CSV file is found

            if csv_file is None:
                print(f"Arquivo CSV não encontrado em {extracted_folder_path}")
                return

            chunk_size = 100000
            for chunk in pd.read_csv(csv_file, encoding="ISO-8859-1", sep=";", chunksize=chunk_size, low_memory=False):
                chunk.columns = csvHeader  # Corrige os cabeçalhos para cada chunk
                counter += 1

                corrected_csv_file = os.path.join(arquivos_folder_path, f"{fileName}_corrigido_chunk.csv")
                chunk.to_csv(corrected_csv_file, index=False, encoding="utf-8", mode='a', header=not os.path.exists(corrected_csv_file))  # Escreve em modo append

                print(f"Arquivo corrigido gerado em: {corrected_csv_file}")
                print(chunk.head())

                # Realiza o upsert após processar o chunk
                upsertCSVIntoBD(fileName, corrected_csv_file)

            os.remove(zip_file_path)
            os.remove(csv_file)
            print(f"Arquivos {zip_file_path} e {csv_file} excluídos.")

    else:
        print(f"Baixando arquivo {fileName}.zip...")

        url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year}-0{month}/{fileName}.zip'
        print(f"URL: {url}")

        try:
            download_with_retries(url, zip_file_path)  # Chamada da função com retry
        except Exception:
            print(f"Erro ao baixar o arquivo {fileName}.zip após várias tentativas.")
            return

        try:
            if not os.path.exists(extracted_folder_path):
                os.makedirs(extracted_folder_path)
                print(f"Pasta de extração criada em: {extracted_folder_path}")

            shutil.unpack_archive(zip_file_path, extracted_folder_path)
            print(f"Arquivo {zip_file_path} extraído com sucesso.")

        except Exception as e:
            print(f"Erro ao extrair o arquivo: {e}")
            return

        extracted_files = os.listdir(extracted_folder_path)
        print(f"Arquivos extraídos: {extracted_files}")
        csv_file = None
        for file in extracted_files:
                csv_file = os.path.join(extracted_folder_path, file)
                break

        if csv_file is None:
            print(f"Arquivo CSV não encontrado em {extracted_folder_path}")
            return

        print(f"Arquivo CSV encontrado: {csv_file}")

        chunk_size = 100000  # Tamanho do chunk (ajuste conforme necessário)
        for chunk in pd.read_csv(csv_file, encoding="ISO-8859-1", sep=";", chunksize=chunk_size, low_memory=False):
            chunk.columns = csvHeader  # Corrige os cabeçalhos para cada chunk
            counter += 1

            corrected_csv_file = os.path.join(arquivos_folder_path, f"{fileName}_corrigido_chunk.csv")
            chunk.to_csv(corrected_csv_file, index=False, encoding="utf-8", mode='a', header=not os.path.exists(corrected_csv_file))  # Escreve em modo append
            print(f"Arquivo corrigido gerado em: {corrected_csv_file}")
            print(chunk.head())

                # Realiza o upsert após processar o chunk
            upsertCSVIntoBD(fileName, corrected_csv_file)

        os.remove(zip_file_path)
        os.remove(csv_file)
        print(f"Arquivos {zip_file_path} e {csv_file} excluídos.")

        # Realiza o upsert após o processamento do arquivo
        upsertCSVIntoBD(fileName, corrected_csv_file)


def file_exists(url):
    """Verifica se o arquivo existe na URL."""
    try:
        response = requests.head(url)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False




def upsertFilesBd():
    "Processa os arquivos na ordem definida"
    order_of_files = [
        'Naturezas',
        'Qualificacoes',
        'Paises',
        'Municipios',
        'Cnaes',
        'Empresas',
        'Estabelecimentos',
        'Simples',
        'Socios'
    ]

    for file_name in order_of_files:
        getFiles(file_name, 2, 2025)

upsertFilesBd()
