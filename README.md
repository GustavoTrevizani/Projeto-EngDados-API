# Projeto-EngDados-API

Este projeto faz parte do curso de Engenharia de Dados da **GrowDev** em parceria com a **Arezzo&CO**. O objetivo é construir um pipeline de ingestão e transformação de dados utilizando uma arquitetura de Data Lake, com processamento em diferentes camadas: **transient**, **bronze** e **silver**.

## Estrutura do Projeto

### Diretórios e Arquivos

- `bronze/`: Contém os dados brutos armazenados após a ingestão inicial.
- `silver/`: Dados transformados e prontos para análises avançadas.
- `gold/`: Dados prontos para serem consumidos em dashboards e análises.

### Scripts

1. **source_to_transient.ipynb**:
   - **Função**: Este script extrai dados diretamente da fonte externa e armazena temporariamente na camada **transient**.
   - **Objetivo**: Capturar os dados brutos para que possam ser processados de forma mais eficiente antes de serem movidos para o bronze.

2. **transient_to_bronze.ipynb**:
   - **Função**: Transfere os dados da camada **transient** para a camada **bronze**, onde são armazenados no Data Lake no formato original.
   - **Objetivo**: Preservar os dados brutos e permitir reprocessamento futuro, se necessário.

3. **bronze_to_silver.ipynb**:
   - **Função**: Este script lê os dados da camada **bronze**, aplica transformações (como limpeza, remoção de duplicatas e padronização) e os move para a camada **silver**.
   - **Objetivo**: Preparar os dados para análises mais avançadas, eliminando inconsistências e garantindo a qualidade.

## Tecnologias Utilizadas

- **Python**: Linguagem de programação principal.
- **PySpark**: Processamento distribuído de dados.
- **AWS S3**: Armazenamento em nuvem para o Data Lake.
- **SQL**: Linguagem para consultas e manipulação de dados.

## Fluxo de Trabalho

1. **Ingestão de Dados (source_to_transient)**: Extração dos dados da fonte externa e armazenamento temporário.
2. **Transferência (transient_to_bronze)**: Movimenta os dados brutos para a camada bronze.
3. **Transformação (bronze_to_silver)**: Limpeza e padronização dos dados, movendo-os para a camada silver.


