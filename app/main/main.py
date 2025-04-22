import shutil
import requests
from datetime import datetime
import os
import pandas as pd
import time
from decimal import Decimal
from sqlalchemy.orm.exc import NoResultFound
from psycopg2 import OperationalError
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from schemas import Company, Establishment, Simples, Partner, LegalNature, City, Country, Cnae, PartnerQualification
from bd import getSession

def getSessionWithRetry(file_name, corrected_csv_file):
    retries = 5
    while retries > 0:
        try:
            # Tenta criar a sessão
              print('tentando se reconectar...')
              session = getSession()
              upsertCSVIntoBD(file_name, corrected_csv_file)
        except OperationalError as e:
            print(f"Erro na conexão com o banco de dados: {e}")
            retries -= 1
            time.sleep(5)  # Espera 5 segundos antes de tentar novamente
    print("Falha ao conectar após várias tentativas.")
    return None

def download_with_retries(url, zip_file_path, max_retries=10, backoff_factor=2):
    attempt = 0
    while attempt < max_retries:
        try:
            print(f"Baixando arquivo (tentativa {attempt + 1}/{max_retries})...")  # Mostra a tentativa
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
            return datetime.strptime(date_str, '%Y%m%d')
        except (ValueError, TypeError):
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
    bulk_data = []  # Lista que vai armazenar os dados a serem inseridos em lote
    row = None
    # Verifica se o campo existe e não é nulo, caso contrário, atribui um valor padrão
    def safe_get(row, column, default_value=""):
      value = row.get(column, default_value)
      if pd.isna(value) or value in ["nan", "NaN", "None", ""]:
          return default_value
      if column == 'PAIS' and value not in ["nan", "NaN", "None", ""]:
        value = int(value)  # Converte apenas o 'value', que é o código do país
        return str(value).strip()

      return str(value).strip()


    try:
        dtype = {
            'DDD 1': 'str',
            'DDD 2': 'str',
            'PAIS': 'str',
            'TELEFONE 1': 'str',
            'TELEFONE 2': 'str',
            'FAX': 'str',
        }

        df = pd.read_csv(corrected_csv_file, encoding='ISO-8859-1', low_memory=False, dtype=dtype)
        print(f"Total de linhas no CSV: {len(df)}")



        # if 'DDD 1' in df.columns:
        #     df['DDD 1'] = df['DDD 1'].fillna(pd.NA).astype('Int64')  # Usar pd.NA para valores nulos
        # if 'DDD 2' in df.columns:
        #     df['DDD 2'] = df['DDD 2'].fillna(pd.NA).astype('Int64')
        if 'TELEFONE 1' in df.columns:
            df['TELEFONE 1'] = df['TELEFONE 1'].fillna('0').astype(str).str.replace(r'\D', '', regex=True)  # Remover não numéricos
        if 'TELEFONE 2' in df.columns:
            df['TELEFONE 2'] = df['TELEFONE 2'].fillna('0').astype(str).str.replace(r'\D', '', regex=True)
        if 'FAX' in df.columns:
            df['FAX'] = df['FAX'].fillna('0').astype(str).str.replace(r'\D', '', regex=True)

        # Tratamento para CEP: Remove espaços e traços
        if 'CEP' in df.columns:
            df['CEP'] = df['CEP'].fillna('0').astype(str).str.replace(r'\D', '', regex=True)

        if 'CORREIO ELETRONICO' in df.columns:
            df['CORREIO ELETRONICO'] = df['CORREIO ELETRONICO'].fillna('')  # Substitui NaN por uma string vazia

        if 'CAPITAL SOCIAL DA EMPRESA' in df.columns:
            df['CAPITAL SOCIAL DA EMPRESA'] = df['CAPITAL SOCIAL DA EMPRESA'].apply(lambda x: str(x).replace(" ", "") if isinstance(x, (str, float, int)) else '0')

        if 'TELEFONE 1' in df.columns:
            df['TELEFONE 1'] = df['TELEFONE 1'].fillna('0').astype(str).str.replace(r'\D', '', regex=True)  # Remover não numéricos
        if 'TELEFONE 2' in df.columns:
            df['TELEFONE 2'] = df['TELEFONE 2'].fillna('0').astype(str).str.replace(r'\D', '', regex=True)
              # Tratamento para FAX e DDD, removendo qualquer caractere não numérico
        if 'FAX' in df.columns:
            df['FAX'] = df['FAX'].fillna('0').astype(str).str.replace(r'\D', '')  # Remove não numéricos

        if 'DDD 1' in df.columns:
            df['DDD 1'] = df['DDD 1'].fillna('0').astype(str).str.replace(r'\D', '')  # Remove não numéricos
        if 'DDD 2' in df.columns:
            df['DDD 2'] = df['DDD 2'].fillna('0').astype(str).str.replace(r'\D', '')  # Remove não numéricos


        # Tratamento para CEP: Remove espaços e traços
        if 'CEP' in df.columns:
            df['CEP'] = df['CEP'].fillna('0').astype(str).str.replace(r'\D', '', regex=True)

        if 'CORREIO ELETRONICO' in df.columns:
            df['CORREIO ELETRONICO'] = df['CORREIO ELETRONICO'].fillna('')  # Substitui NaN por uma string vazia

        if 'CAPITAL SOCIAL DA EMPRESA' in df.columns:
            df['CAPITAL SOCIAL DA EMPRESA'] = df['CAPITAL SOCIAL DA EMPRESA'].apply(lambda x: str(x).replace(" ", "") if isinstance(x, (str, float, int)) else '0')

        # Processar colunas de data com tratamento adequado para NaN
        for col in ['DATA DE INICIO DE ATIVIDADE', 'DATA DA SITUACAO ESPECIAL']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')

        if 'PAIS' in df.columns:
            # Preenche valores nulos com '105' e remove qualquer valor não numérico
            df['PAIS'] = df['PAIS'].fillna('105').astype(str).str.replace(r'\D', '')  # Remove qualquer caractere não numérico
            df['PAIS'] = df['PAIS'].apply(lambda x: x if x.isdigit() else '105')  # Garante que só números permaneçam




        if 'DDD 1' in df.columns:
            #df['DDD 1'] = df['DDD 1'].replace('0', pd.NA).fillna(pd.NA).astype('Int64')
            df['DDD 1'] = df['DDD 1'].fillna('0').astype(str).str.replace(r'\D', '')  # Remove qualquer caractere não numérico
        if 'DDD 2' in df.columns:
            #df['DDD 2'] = df['DDD 2'].replace('0', pd.NA).fillna(pd.NA).astype('Int64')
            df['DDD 2'] = df['DDD 2'].fillna('0').astype(str).str.replace(r'\D', '')  # Remove qualquer caractere não numérico
        if 'TELEFONE 1' in df.columns:
            df['TELEFONE 1'] = df['TELEFONE 1'].fillna('0').astype(str).str.replace(r'\D', '')  # Remove qualquer caractere não numérico
            df['TELEFONE 1'] = df['TELEFONE 1'].fillna('0').astype(str).str.replace(" ", '')
            df['TELEFONE 1'] = df['TELEFONE 1'].fillna('0').astype(str).str.replace("-", '')
            df['TELEFONE 1'] = df['TELEFONE 1'].apply(lambda x: str(int(float(x))) if x != '0' else '0')  # Converte para número
        if 'TELEFONE 2' in df.columns:
            df['TELEFONE 2'] = df['TELEFONE 2'].fillna('0').astype(str).str.replace(r'\D', '')  # Remove qualquer caractere não numérico
            df['TELEFONE 2'] = df['TELEFONE 2'].fillna('0').astype(str).str.replace(" ", '')
            df['TELEFONE 2'] = df['TELEFONE 2'].fillna('0').astype(str).str.replace("-", '')
            df['TELEFONE 2'] = df['TELEFONE 2'].apply(lambda x: str(int(float(x))) if x != '0' else '0')  # Converte para número
        if 'FAX' in df.columns:
            df['FAX'] = df['FAX'].fillna(0).astype(str).str.replace(" ", "").apply(lambda x: str(int(float(x))) if x != '0' else '0')
        if 'CEP' in df.columns:
            df['CEP'] = df['CEP'].fillna(0).astype(float).astype(int).astype(str).str.replace(' ', '').replace("-", "")


        if 'CORREIO ELETRONICO' in df.columns:
            df['CORREIO ELETRONICO'] = df['CORREIO ELETRONICO'].fillna('')  # Substitui NaN por uma string vazia

        if 'CAPITAL SOCIAL DA EMPRESA' in df.columns:
            df['CAPITAL SOCIAL DA EMPRESA'] = df['CAPITAL SOCIAL DA EMPRESA'].apply(lambda x: str(x).replace(" ", "") if isinstance(x, (str, float, int)) else x)

        # Processar dados de data, tratando valores NaN com None
        for col in ['DATA DE INICIO DE ATIVIDADE', 'DATA DA SITUACAO ESPECIAL']:
              if col in df.columns:
                  df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')

        if 'CNPN BASICO' in df.columns:
            df['CNPJ BASICO'] = df['CNPJ BASICO'].astype(str).str.replace(' ', '')

        chunk_size = 100000  # Tamanho do chunk
        for chunk in pd.read_csv(corrected_csv_file, encoding='utf-8', low_memory=False, chunksize=chunk_size):
            print(f"Processando chunk com {len(chunk)} registros...")
            record_counter = 0
            for index, row in chunk.iterrows():
                row = row.to_dict()
                print(f'total de registros até o momento inseridos {inserted_count}')
                if 'Empresas' in file_name:
                    company = Company(
                        base_cnpj=str(row['CNPJ BASICO']),
                        social_reason_business_name=row['RAZAO SOCIAL/NOME EMPRESARIAL'],
                        legal_nature=row['NATUREZA JURIDICA'],
                        responsible_qualification=str(row['QUALIFICACAO DO RESPONSAVEL']),
                        social_capital_company=row['CAPITAL SOCIAL DA EMPRESA'].replace(',', '.'),
                        company_size=row['PORTE DA EMPRESA'],
                        responsible_federative_entity=row['ENTE FEDERATIVO RESPONSÁVEL'],
                        updated_at=date
                    )
                    print(f'passou por empresa: \n{company}')
                    try:
                        bulk_data.append(company)  # 'merge' faz o upsert
                    except Exception as e:
                        print(f'erro: {e}')
                        session.rollback()  # Reverte a transação se houver um erro
                        getSessionWithRetry(file_name, corrected_csv_file)
                    print('passou pelo merge')
                    inserted_count += 1  # Incrementa o contador

                elif 'Estabelecimento' in file_name:
                    # Verifica se a empresa existe no banco de dados
                    # company_cnpj = str(row['CNPJ BASICO']).strip()
                    # base_cnpj = None
                    # try:
                    #     company = session.query(Company).filter_by(base_cnpj=company_cnpj).one()
                    #     base_cnpj = company.base_cnpj  # Atribui o base_cnpj se a empresa existir
                    # except NoResultFound:
                    #     base_cnpj = None  # Se a empresa não for encontrada, base_cnpj será None

                    establishment = Establishment(
                        base_cnpj=safe_get(row, 'CNPJ BASICO', None),
                        cnpj_dv=safe_get(row, 'CNPJ DV'),
                        cnpj_order=safe_get(row, 'CNPJ ORDEM'),
                        fantasy_name=safe_get(row, 'NOME FANTASIA'),
                        identifier_branch_matriz=safe_get(row, 'IDENTIFICADOR MATRIZ/FILIAL', ''),
                        cadastral_situation=safe_get(row, 'SITUACAO CADASTRAL', ''),
                        cadastral_situation_reason=safe_get(row, 'MOTIVO SITUACAO CADASTRAL', ''),
                        city_name_exterior=safe_get(row, 'NOME DA CIDADE NO EXTERIOR', ''),
                        country=safe_get(row, 'PAIS', '105'),
                        activity_start_date=parse_date(safe_get(row, 'DATA DE INICIO DE ATIVIDADE', '1900-01-01')),
                        special_situation_date=parse_date(safe_get(row, 'DATA DA SITUACAO ESPECIAL', '1900-01-01')),
                        cnae_main=safe_get(row, 'CNAE FISCAL PRINCIPAL', ''),
                        street_type=safe_get(row, 'TIPO DE LOGRADOURO', ''),
                        street=safe_get(row, 'LOGRADOURO', ''),
                        number=safe_get(row, 'NUMERO', ''),
                        complement=safe_get(row, 'COMPLEMENTO', ''),
                        neighborhood=safe_get(row, 'BAIRRO', ''),
                        cep=safe_get(row, 'CEP', ''),
                        cnpj=f"{safe_get(row, 'CNPJ BASICO')}000{safe_get(row, 'CNPJ ORDEM')}{safe_get(row, 'CNPJ DV')}",
                        city=safe_get(row, 'MUNICIPIO', ''),
                        ddd_1=safe_get(row, 'DDD 1', ''),
                        phone_1=safe_get(row, 'TELEFONE 1', '').replace(" ", ""),
                        ddd_2=safe_get(row, 'DDD 2', ''),
                        phone_2=safe_get(row, 'TELEFONE 2', '').replace(" ",""),
                        fax_ddd=safe_get(row, 'DDD DO FAX', ''),
                        fax=safe_get(row, 'FAX', '').replace(" ", ""),
                        electronic_mail=safe_get(row, 'CORREIO ELETRONICO', ''),
                        special_situation=safe_get(row, 'SITUACAO ESPECIAL', ''),
                        uf=safe_get(row, 'UF', ''),
                        #cnae_secondary=safe_get(row, 'CNAE FISCAL SECUNDARIA', None),
                        updated_at=datetime.now()  # Data e hora atuais
                    )
                    bulk_data.append(establishment)
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
                    bulk_data.append(partner)
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
                    bulk_data.append(simples)
                    inserted_count += 1

                elif 'Cnaes' in file_name:
                    cnae = Cnae(
                        code=str(row['CODIGO']),
                        description=row['DESCRICAO'],
                        updated_at=date
                    )
                    bulk_data.append(cnae)
                    inserted_count += 1

                elif 'Naturezas' in file_name:
                    nature = LegalNature(
                        code=str(row['CODIGO']),
                        description=row['DESCRICAO'],
                        updated_at=date
                    )
                    bulk_data.append(nature)
                    inserted_count += 1

                elif 'Qualificacoes' in file_name:
                    partnerQuali = PartnerQualification(
                        code=str(row['CODIGO']),
                        description=row['DESCRICAO'],
                        updated_at=date
                    )
                    bulk_data.append(partnerQuali)
                    inserted_count += 1

                elif 'Municipios' in file_name:
                    city = City(
                        code=str(row['CODIGO']),
                        description=row['DESCRICAO'],
                        updated_at=date
                    )
                    bulk_data.append(city)
                    inserted_count += 1

                elif 'Paises' in file_name:
                    country = Country(
                        code=str(row['CODIGO']),
                        description=row['DESCRICAO'],
                        updated_at=date
                    )
                    bulk_data.append(country)
                    inserted_count += 1

                # Dentro do loop principal, onde processa os chunks:
                if len(bulk_data) >= 100000:
                    try:
                        session.bulk_save_objects(bulk_data)
                        session.commit()
                        print(f'{len(bulk_data)} registros inseridos com sucesso.')
                    except IntegrityError as e:
                        session.rollback()
                        print(f"Erro de integridade: {e}")
                    except Exception as e:
                        session.rollback()
                        print(f"Erro ao inserir dados: {e}")
                    finally:
                        bulk_data = []


            # Comitar quaisquer registros restantes que não atingiram o tamanho do lote
            # Após o loop de chunks, para o último lote não processado:
            if bulk_data:
                try:
                    session.bulk_save_objects(bulk_data)
                    session.commit()
                    #print(f'{len(bulk_data)} registros inseridos com sucesso.')
                except IntegrityError as e:
                    session.rollback()
                    print(f"Erro de integridade: {e}")
                    return
                except Exception as e:
                    session.rollback()
                    print(f"Erro ao inserir dados restantes: {e}")
                    return
                finally:
                    bulk_data = []  # Garante que o bulk_data seja esvaziado



        print(f"Dados do arquivo {file_name} inseridos/atualizados com sucesso no banco de dados.")
    except Exception as e:
        print(f'Erro ao processar o arquivo {file_name}: {row} : {e}')
        session.rollback()
        return



def getFiles(fileName, month, year):
    """
    Baixa e processa os arquivos para inserção no banco de dados, realizando um upsert para cada um.

    :param fileName: Nome do arquivo (para identificar o tipo de dados).
    :param month: Mês do arquivo a ser processado.
    :param year: Ano do arquivo a ser processado.
    """
    csvHeader = []
    now = datetime.now()
    counter = 4
    csv_counter = 1

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

    extracted_folder_path = os.path.join(arquivos_folder_path, 'extracted')

    if fileName in ['Empresas', 'Estabelecimentos', 'Socios']:
        while True:
            # Definir o nome do arquivo e o URL para cada iteração com base no counter
            zip_file_path = os.path.join(arquivos_folder_path, f"{fileName}_{counter}.zip")

            print(f"Verificando se o arquivo {fileName}_{counter}.zip já existe...")

            # Verifica se o arquivo já foi baixado
            if os.path.exists(zip_file_path):
                print(f"O arquivo {fileName}_{counter}.zip já foi baixado. Pulando download.")
            else:
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

            # Extração e processamento do arquivo baixado
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
                file_path = os.path.join(extracted_folder_path, file)
                if os.path.isfile(file_path) and file.endswith("CSV") or file.endswith('ESTABELE'):
                    csv_file = file_path
                    print(f'CSV encontrado: {csv_file}')
                    break

            if csv_file is None:
                print(f"Arquivo CSV não encontrado em {extracted_folder_path}")
                return

            chunk_size = 100000  # Tamanho da chunk

            # Carregar o CSV original inteiro
            # Ler o arquivo em chunks e processar cada um
            for chunk in pd.read_csv(csv_file, encoding="ISO-8859-1", sep=";", chunksize=chunk_size, low_memory=False):
                chunk.columns = csvHeader
                corrected_csv_file = os.path.join(arquivos_folder_path, f"{fileName}_chunk_{csv_counter}.csv")
                chunk.to_csv(corrected_csv_file, index=False, encoding="utf-8", mode='w')
                print(f"Arquivo corrigido gerado em: {corrected_csv_file}")
                print(chunk.head())
                upsertCSVIntoBD(fileName, corrected_csv_file)
                csv_counter += 1
                os.remove(corrected_csv_file)

            os.remove(zip_file_path)
            os.remove(csv_file)
            print(f"Arquivos {zip_file_path} e {csv_file} excluídos.")

            counter += 1  # Incrementa o counter corretamente para o próximo arquivo

    else:
      zip_file_path = os.path.join(arquivos_folder_path, f"{fileName}.zip")
      print(f"Verificando se o arquivo {fileName}.zip já existe...")

      if os.path.exists(zip_file_path):
          print(f"O arquivo {fileName}.zip já foi baixado. Pulando download.")
      else:
          print(f"Baixando arquivo {fileName}.zip...")
          url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year}-0{month}/{fileName}.zip'
          print(f"URL: {url}")

          try:
              download_with_retries(url, zip_file_path)
          except Exception as e:
              print(f"Erro ao baixar o arquivo {fileName}.zip após várias tentativas. {e}")
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
          file_path = os.path.join(extracted_folder_path, file)
          if os.path.isfile(file_path):
              csv_file = file_path
              break

      if csv_file is None:
          print(f"Arquivo CSV não encontrado em {extracted_folder_path}")
          return

      print(f"Arquivo CSV encontrado: {csv_file}")
      chunk_size = 100000
      csv_counter = 1
      for chunk in pd.read_csv(csv_file, encoding="ISO-8859-1", sep=";", chunksize=chunk_size, low_memory=False):
          chunk.columns = csvHeader
          #print(f"Processando chunk com {len(chunk)} linhas")
          #psertCSVIntoBD(fileName, df=chunk)
          corrected_csv_file = os.path.join(arquivos_folder_path, f"{fileName}_chunk_{csv_counter}.csv")
          chunk.to_csv(corrected_csv_file, index=False, encoding="utf-8", mode='w')
          print(f"Arquivo corrigido gerado em: {corrected_csv_file}")
          print(chunk.head())
          upsertCSVIntoBD(fileName, corrected_csv_file)
          csv_counter += 1
          os.remove(corrected_csv_file)

      os.remove(zip_file_path)
      os.remove(csv_file)
      print(f"Arquivos {zip_file_path} e {csv_file} excluídos.")


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
        #'Naturezas',
        #'Qualificacoes',
        #'Paises',
        #'Municipios',
        #'Cnaes',
        #'Empresas',
        #'Estabelecimentos',
        'Simples',
        #'Socios'
    ]

    for file_name in order_of_files:
        getFiles(file_name, 3, 2025)

upsertFilesBd()
