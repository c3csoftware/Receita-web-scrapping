<!-- Author: Victor Sales -->

<br />
<div align="center">
  <a href="https://github.com/c3csoftware">
    <img src="c3clogo.png" alt="Logo" width="80" height="80">
  </a>

<h3 align="center">C3C API Para Consultar CNPJ's</h3>

  <p align="center">
    Biblioteca para chamadas à API's
    <br />
    <br />
    <br />
    ·
    <a href="https://github.com/c3csoftware/C3C-CNPJInsights/issues/new?labels=bug&template=bug-report---.md">Report de Bugs</a>
    ·
    <a href="https://github.com/c3csoftware/C3C-CNPJInsights/issues/new?labels=enhancement&template=feature-request---.md">Sugerir nova Feature</a>
  </p>
</div>

---

## Visão Geral

Esta biblioteca permite realizar consultas automatizadas a API's para obter dados relacionados a CNPJ's de empresas, estabelecimentos e sócios. O processo é contínuo e baseado na extração de dados indexados, seguida pela inserção em um banco de dados para posterior análise e consulta.

### Funcionalidades

- **Consulta de dados na receita federal.**
- **Inserção automatizada no banco de dados Postgress.**
- **Processamento contínuo dos dados até que todos os arquivos sejam baixados.**

---

## Workflow da Aplicação

O fluxo principal da aplicação segue o processo descrito abaixo:

1. **Download dos Arquivos Indexados:**
   - O sistema começa buscando os arquivos de índices relacionados às empresas. Cada índice contém informações específicas que são consultadas em lotes.
   - A busca ocorre até que todos os arquivos de empresas sejam processados, ou seja, até não haver mais arquivos indexados disponíveis para download.
   
2. **Inserção dos Dados no Banco de Dados:**
   - Após o download de cada arquivo de índice, os arquivos extraídos são trasformados em arquivos .csv com os devidos cabeçalhos para cada arquivo (CONFORME O LAYOUT DA RECEITA: <a href="https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf"></a>) os dados são lidos e inseridos no banco de dados

3. **Processamento Contínuo:**
   - O sistema executa esse ciclo de forma contínua, até que todos os arquivos de empresas, estabelecimentos e sócios sejam baixados e inseridos no banco.
   - O objetivo final é garantir que todas as informações estejam centralizadas no banco de dados para consultas futuras.

---

## Estrutura do Projeto

- **main.py:** Script principal que controla o fluxo de execução.
- **db.py:** Módulo responsável pela inserção e gestão dos dados no banco de dados (Sessões).
- **schemas.py:** Módulo que reflete como as tabelas estão estruturadas no banco de dados
- **.env:** Arquivo de configuração para URLs do banco.

---

## Configuração do Projeto

### Dependências
  
Instale todas as dependências utilizando o pip:

```bash
pip install -r requirements.txt
