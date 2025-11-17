# Sistema de Catalogação de Dados - Apache Atlas

Sistema de catalogação automática de dados que integra PostgreSQL com Apache Atlas.

## 📋 Descrição

Este projeto implementa:
- Extração de metadados de bancos de dados PostgreSQL
- Catalogação automática no Apache Atlas
- Criação de linhagem de dados baseada em relacionamentos
- Geração de relatórios em JSON e CSV

## 📁 Estrutura do Projeto

```
lab/
├── atlas_client.py          # Tarefa 1: Cliente Apache Atlas
├── postgres_extractor.py    # Tarefa 2: Extrator de metadados PostgreSQL
├── data_catalogger.py       # Tarefa 3: Catalogador automático
├── discovery_report.py      # Tarefa 4: Gerador de relatórios
├── main.py                  # Script principal
├── config.py                # Configurações
├── requirements.txt         # Dependências
└── README.md               # Este arquivo
```

## 🚀 Instalação

### 1. Iniciar Ambiente Docker

```bash
cd atlas-dataops-lab
docker-compose up -d

# Aguardar inicialização (5-10 minutos)
docker-compose logs -f atlas
```

### 2. Instalar Dependências

```bash
cd lab
pip install -r requirements.txt
```

### 3. Verificar Serviços

- **Apache Atlas**: http://localhost:21000 (admin/admin)
- **PostgreSQL**: localhost:2001 (postgres/postgres)

## 📖 Uso

### Executar Sistema Completo

```bash
python main.py
```

Isso irá:
1. Conectar ao PostgreSQL e extrair metadados
2. Catalogar todas as tabelas e colunas no Atlas
3. Criar linhagem de dados baseada em foreign keys
4. Gerar relatórios em JSON e CSV

### Uso Programático

```python
from atlas_client import AtlasClient
from postgres_extractor import PostgreSQLExtractor
from data_catalogger import DataCatalogger
from discovery_report import DiscoveryReport
from config import ATLAS_CONFIG, POSTGRES_CONFIG

# Inicializar componentes
atlas = AtlasClient(**ATLAS_CONFIG)
extractor = PostgreSQLExtractor(**POSTGRES_CONFIG)
catalogger = DataCatalogger(atlas, extractor)

# Catalogar dados
results = catalogger.catalog_all_tables()
print(f"{results['tables_created']} tabelas catalogadas")

# Gerar relatório
report = DiscoveryReport(atlas)
report.generate_report("discovery_report")
```

## 📊 Saída

O sistema gera 3 arquivos de relatório:

- `discovery_report.json` - Relatório completo em JSON
- `discovery_report_tables.csv` - Tabelas catalogadas
- `discovery_report_relationships.csv` - Relacionamentos/linhagens

## 🔧 Configuração

As configurações estão em `config.py`:

```python
ATLAS_CONFIG = {
    "url": "http://localhost:21000",
    "username": "admin", 
    "password": "admin"
}

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 2001,
    "database": "northwind",
    "user": "postgres",
    "password": "postgres"
}
```

## 📦 Dependências

- `requests>=2.28.0` - Cliente HTTP para API do Atlas
- `pandas>=1.5.0` - Manipulação de dados
- `psycopg2-binary>=2.9.0` - Conector PostgreSQL

## 🎯 Funcionalidades Implementadas

### AtlasClient (Tarefa 1)
- Conexão com Apache Atlas via API REST
- Autenticação HTTP Basic
- Métodos: `search_entities()`, `create_entity()`, `get_entity()`, `get_lineage()`
- Tratamento de erros HTTP

### PostgreSQLExtractor (Tarefa 2)
- Conexão segura com PostgreSQL
- Extração de metadados completos (tabelas, colunas, tipos, nullable)
- Identificação de chaves primárias
- Extração de relacionamentos (foreign keys)

### DataCatalogger (Tarefa 3)
- Integração entre PostgreSQL e Atlas
- Criação hierárquica: Database → Tabela → Coluna
- Método `catalog_all_tables()` para catalogação automática
- Criação de linhagem entre tabelas relacionadas

### DiscoveryReport (Tarefa 4)
- Geração de relatórios das entidades catalogadas
- Estatísticas: total de databases, tabelas, colunas
- Identificação de tabelas com mais colunas
- Listagem de relacionamentos
- Exportação em JSON e CSV

## 🐛 Troubleshooting

### Atlas não está acessível
```bash
# Verificar containers
docker ps

# Ver logs do Atlas
docker-compose logs -f atlas
```

### Erro de conexão com PostgreSQL
```bash
# Verificar se container está rodando
docker ps | grep postgres
```

## 📄 Licença

Projeto desenvolvido para fins educacionais.
