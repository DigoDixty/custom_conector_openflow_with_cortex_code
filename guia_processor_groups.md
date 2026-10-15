# Guia: Processor Groups - Openflow

> Baseado no export `Openflow.json` — runtime `runtime-sqlserver`
---

## Arquitetura Geral

```
Openflow (Root)
├── FULL_MANUAL_SQLServer_to_Snowflake      (carga manual, tabela específica)
├── SQLServer_to_Snowflake - FULL           (pipeline automatizado FULL com logs)
├── INCREMENT_MANUAL_SQLServer_to_Snowflake (carga manual incremental)
├── SQLServer_to_Snowflake Incremental      (pipeline automatizado incremental com logs)
└── SQLServer                               (CDC connector nativo - Change Tracking)
```

---

## Objetos de Suporte no Snowflake

### Tabela de Configuração

```sql
-- DB_HML_STAGE.SETTINGS.FULL_LOAD
CREATE OR REPLACE TABLE DB_HML_STAGE.SETTINGS.FULL_LOAD (
    SOURCE_HOST              VARCHAR(255),
    SOURCE_DATABASE          VARCHAR(255),
    SOURCE_SCHEMA            VARCHAR(255),
    SOURCE_TABLE             VARCHAR(255),
    TARGET_DATABASE          VARCHAR(255),
    TARGET_SCHEMA            VARCHAR(255),
    TARGET_TABLE             VARCHAR(255),
    IS_ACTIVE                BOOLEAN,
    LOAD_TYPE                VARCHAR(50),       -- FULL | INCREMENTAL | REPROC_FULL
    CREATED_AT               TIMESTAMP_NTZ(9),
    ROW_COUNT                NUMBER(38,0),
    LAST_DT_LOAD             TIMESTAMP_NTZ(9),
    DURATION_MILISSECONDS    NUMBER(38,0),
    COLUMN_NAME_INCREMENT    VARCHAR,           -- coluna watermark (ex: UpdatedAt)
    COLUMN_VALUE             VARCHAR,           -- último valor watermark
    COLUMN_ID                VARCHAR,           -- PK para MERGE
    RAW_DB                   VARCHAR,
    COLUMN_VALUE_MAX         VARCHAR
);
```

### View de Configuração (gera SQL dinâmico)

```sql
-- DB_HML_STAGE.SETTINGS.VW_FULL_LOAD
-- Gera o SQL_QUERY automaticamente baseado em LOAD_TYPE e SOURCE_HOST:
--   FULL/REPROC_FULL: SELECT * FROM {db}.dbo.{table} WITH (NOLOCK)
--   INCREMENTAL:      SELECT * FROM {db}.dbo.{table} WHERE {col} > DATEADD(...)
```

### Tabela de Logs

```sql
-- DB_HML_STAGE.SETTINGS.PIPELINE_LOGS
CREATE OR REPLACE TABLE DB_HML_STAGE.SETTINGS.PIPELINE_LOGS (
    SOURCE_HOST     VARCHAR,
    DATABASE_NAME   VARCHAR,
    SCHEMA_NAME     VARCHAR,
    TABLE_NAME      VARCHAR,
    STATUS          VARCHAR,       -- SUCCESS | ERROR
    PROCESSOR       VARCHAR,
    ROW_COUNT       NUMBER(38,0),
    ERROR_MESSAGE   VARCHAR,
    LOG_DATETIME    TIMESTAMP_NTZ(9),
    LOAD_TYPE       VARCHAR,
    MESSAGE         VARCHAR
);
```

### Stored Procedures

| Procedure | Chamada no Openflow | Função |
|-----------|-------------------|--------|
| `SP_LOAD_FULL_TABLES` | `CALL DB_HML_STAGE.DATABASE.SP_LOAD_FULL_TABLES('${source.table}')` | Compara HASH_AGG entre STAGE e RAW; se diferente, TRUNCATE+INSERT na RAW |
| `SP_INCREMENTAL_LOAD` | `CALL DB_HML_STAGE.DATABASE.SP_INCREMENTAL_LOAD('${source.table}')` | Lê Stream, insere em transient, faz MERGE na RAW |
| `SP_MERGE_INCREMENTAL` | Chamada pós-ingestão | MERGE deduplicado com ROW_NUMBER por COLUMN_ID |

---

## Controller Services (compartilhados em cada PG)

| Nome | Tipo | Configuração |
|------|------|-------------|
| **SQLServer Connection Pool** | DBCPConnectionPool | URL: `jdbc:sqlserver:/hostname:1433;encrypt=false;trustServerCertificate=false` / User: `user_snowflake` / Driver: `#{mssql-jdbc-12.10.0.jre11.jar}` / Max Connections: `20` |
| **Snowflake_Connection** | SnowflakeConnectionService | Database: `DB_HML_STAGE` / Schema: `RAW` / Warehouse: `WH_OPENFLOW` / Auth: `SNOWFLAKE_MANAGED` / Max Connections: `30` |
| **Avro Reader** | AvroReader | Schema: `embedded-avro-schema` / Cache: `1000` |
| **JSON Reader** | JsonTreeReader | Schema: `infer-schema` / Max String: `20 MB` |
| **JSON Writer** | JsonRecordSetWriter | Schema: `inherit-record-schema` / Output: `output-array` / Compress: `none` |
| **JSON Writer Compress** | JsonRecordSetWriter | Igual ao anterior + Compression: `gzip` / Level: `9` |

---

## PG 1: FULL_MANUAL_SQLServer_to_Snowflake

**Uso:** Carga FULL manual para uma tabela específica (sem logs/métricas avançadas).

### Fluxo

```
Trigger Pipeline
    → Query Config Table (lê VW_FULL_LOAD FULL)
    → Convert Avro to JSON
    → Split Config Records (1 registro por FlowFile)
    → Extract SQL Query (extrai atributos: sql.query, source.table, target.schema)
    → Capture Start Time (seta start.time)
    → Truncate Table (trunca tabela destino no Snowflake)
    → Execute SQL Queries (executa SELECT na origem via JDBC)
    → InsertToSnowflake (PutDatabaseRecord no Snowflake)
    → ExecuteSQL: CALL SP_LOAD_FULL_TABLES('${source.table}')
```

### Processors

| # | Nome | Tipo | Configuração chave |
|---|------|------|-------------------|
| 1 | Trigger Pipeline | GenerateFlowFile | Schedule manual ou CRON |
| 2 | Query Config Table | ExecuteSQL | `#{SQL Query FULL}` via Snowflake_Connection |
| 3 | Convert Avro to JSON | ConvertRecord | Avro Reader → JSON Writer |
| 4 | Split Config Records | SplitJson | `$` (split array em registros individuais) |
| 5 | Extract SQL Query | EvaluateJsonPath | Extrai: `$.SQL_QUERY` → `sql.query`, `$.SOURCE_TABLE` → `source.table`, `$.TARGET_SCHEMA` → `target.schema` |
| 6 | Capture Start Time | UpdateAttribute | `start.time = ${now():toNumber()}` |
| 7 | Truncate Table | PutSQL | Trunca `${target.schema}.${source.host}_${source.database}_DBO_${target.table}` |
| 8 | Execute SQL Queries | ExecuteSQLRecord | `${sql.query}` via SQLServer Connection Pool |
| 9 | InsertToSnowflake | PutDatabaseRecord | Table: `${source.host}_${source.database}_DBO_${target.table}` / Schema: `${target.schema}` / Statement: INSERT |
| 10 | ExecuteSQL (SP) | ExecuteSQL | `CALL DB_HML_STAGE.DATABASE.SP_LOAD_FULL_TABLES('${source.table}')` via Snowflake |

### Conexões

```
Trigger Pipeline        --[success]--> Query Config Table
Query Config Table      --[success]--> Convert Avro to JSON
Convert Avro to JSON    --[success]--> Split Config Records
Split Config Records    --[split]----> Extract SQL Query
Extract SQL Query       --[matched]--> Capture Start Time
Capture Start Time      --[success]--> Truncate Table
Truncate Table          --[success]--> Execute SQL Queries
Execute SQL Queries     --[success]--> InsertToSnowflake
InsertToSnowflake       --[success]--> ExecuteSQL (SP)
```

---

## PG 2: SQLServer_to_Snowflake - FULL (Automatizado com Logs)

**Uso:** Pipeline FULL completo com contagem de linhas, cálculo de duração e atualização da tabela de configuração.

### Fluxo

```
Trigger Pipeline
    → Query Config Table
    → Convert Avro to JSON
    → Split Config Records
    → Extract SQL Query
    → Capture Start Time
        ├─→ Truncate Table → Execute SQL Queries → InsertToSnowflake
        │       └─→ [success] → Funnel → ExecuteSQL (SP) → Calculate Duration → Update Config Table
        │       └─→ [failure] → LogMessage
        └─→ ExecuteSQL (COUNT) → Convert Count to JSON → Extract Source Count → Log Success
```

### Processors adicionais (vs FULL_MANUAL)

| Nome | Tipo | Função |
|------|------|--------|
| ExecuteSQL (COUNT) | ExecuteSQL | `SELECT COUNT(1) as ROW_COUNT FROM ${source.database}.dbo.${source.table} with (nolock)` — conta linhas na origem via SQLServer |
| Convert Count to JSON | ConvertRecord | Converte resultado Avro do COUNT em JSON |
| Extract Source Count | EvaluateJsonPath | Extrai `$.ROW_COUNT` → `source.row_count` |
| Log Success | PutSQL | Insere log de sucesso em PIPELINE_LOGS |
| Calculate Duration | UpdateAttribute | `execution.time = ${now():toNumber():minus(${start.time})}` |
| Update Config Table | PutSQL | Atualiza `FULL_LOAD` com `ROW_COUNT`, `LAST_DT_LOAD`, `DURATION_MILISSECONDS` |
| LogMessage | LogMessage | Loga erros de failures |
| Funnel | Funnel | Ponto de convergência entre InsertToSnowflake → ExecuteSQL (SP) |

### Conexões de Erro

```
InsertToSnowflake       --[failure]--> LogMessage
Execute SQL Queries     --[failure]--> LogMessage
Truncate Table          --[failure]--> LogMessage
ExecuteSQL (COUNT)      --[failure]--> LogMessage
ExecuteSQL (SP)         --[failure]--> LogMessage
```

---

## PG 3: INCREMENT_MANUAL_SQLServer_to_Snowflake

**Uso:** Carga incremental manual para tabela específica.

### Fluxo

```
Trigger Pipeline
    → Query Config Table (VW_FULL_LOAD WHERE LOAD_TYPE='INCREMENTAL')
    → Convert Avro to JSON
    → Split Config Records
    → Extract SQL Query
    → Capture Start Time
    → Execute SQL Queries (SELECT com WHERE watermark > valor)
    → InsertToSnowflake (INSERT append, sem truncate)
    → ExecuteSQL: CALL SP_INCREMENTAL_LOAD('${source.table}')
    → Calculate Duration
    → Update Config Table
```

### Diferenças vs FULL

| Aspecto | FULL | INCREMENTAL |
|---------|------|-------------|
| SQL Query | `SELECT * FROM db.dbo.table WITH (NOLOCK)` | `SELECT * FROM db.dbo.table WHERE col > watermark` |
| Antes do INSERT | TRUNCATE TABLE | Nenhum (append) |
| Pós-INSERT SP | `SP_LOAD_FULL_TABLES` | `SP_INCREMENTAL_LOAD` (Stream → Transient → MERGE na RAW) |
| Table Name | `${source.host}_${source.database}_DBO_${target.table}` | `BD_PROD_${source.database}_DBO_${target.table}` |

### Configuração InsertToSnowflake

| Propriedade | Valor |
|-------------|-------|
| Table Name | `BD_PROD_${source.database}_DBO_${target.table}` |
| Schema Name | `${target.schema}` |
| Statement Type | `INSERT` |
| Record Reader | Avro Reader |

---

## PG 4: SQLServer_to_Snowflake Incremental (Automatizado com Logs)

**Uso:** Pipeline incremental completo com contagem, duração, logs de erro e atualização de config.

### Fluxo (mesmo padrão do PG2, mas incremental)

```
Trigger Pipeline
    → Query Config Table
    → Convert Avro to JSON
    → Split Config Records
    → Extract SQL Query
    → Capture Start Time
        ├─→ Execute SQL Queries → InsertToSnowflake
        │       └─→ [success] → Funnel → ExecuteSQL (SP) → Calculate Duration → Update Config Table
        │       └─→ [failure] → LogMessage
        └─→ ExecuteSQL (COUNT) → Convert Count to JSON → Extract Source Count → Log Success
```

### Conexões

```
Trigger Pipeline        --[success]--> Query Config Table
Query Config Table      --[success]--> Convert Avro to JSON
Convert Avro to JSON    --[success]--> Split Config Records
Split Config Records    --[split]----> Extract SQL Query
Extract SQL Query       --[matched]--> Capture Start Time
Capture Start Time      --[success]--> Execute SQL Queries
Capture Start Time      --[success]--> ExecuteSQL (COUNT)
Execute SQL Queries     --[success]--> InsertToSnowflake
Execute SQL Queries     --[failure]--> LogMessage
InsertToSnowflake       --[success]--> Funnel
InsertToSnowflake       --[failure]--> LogMessage
Funnel                  --[       ]--> ExecuteSQL (SP_INCREMENTAL_LOAD)
ExecuteSQL (SP)         --[success]--> Calculate Duration
ExecuteSQL (SP)         --[failure]--> LogMessage
Calculate Duration      --[success]--> Update Config Table
ExecuteSQL (COUNT)      --[success]--> Convert Count to JSON
Convert Count to JSON   --[success]--> Extract Source Count
Extract Source Count    --[matched]--> Log Success
```

---

## PG 5: SQLServer (CDC Connector Nativo)

**Uso:** Replicação CDC via Change Tracking do SQL Server (conector Snowflake gerenciado).

### Controller Services Específicos

| Nome | Tipo | Função |
|------|------|--------|
| Snowflake Connection Pool | SnowflakeConnectionService | Auth SNOWFLAKE_MANAGED + Private Key Service |
| Snowflake Private Key Service | StandardPrivateKeyService | Chave privada para autenticação |
| SQLServer Connection Pool | DBCPConnectionPool | JDBC com parâmetros dinâmicos (`#{SQLServer Connection URL}`, etc.) |
| IncrementalLoadRoutingSemaphore | MultiDatabaseStandardRoutingSemaphore | Controle de concorrência |
| Table Column Filter | MultiDatabaseJsonTableColumnFilter | Filtro de colunas por tabela (`#{Column Filter JSON}`) |
| Source Table Schema Registry | MultiDatabaseStateManagedCdcSchemaRegistry | Registry de schemas para CDC |
| Table State Store | MultiDatabaseStandardTableStateService | Estado de replicação por tabela |
| JSON Writer | JsonRecordSetWriter | Output one-line (não array) |
| JSON Reader | JsonTreeReader | Infer schema |
| StandardWebClientServiceProvider | StandardWebClientServiceProvider | HTTP/2 client |

### Parâmetros do Conector (Parameter Context)

| Parâmetro | Descrição |
|-----------|-----------|
| `#{SQLServer Connection URL}` | JDBC URL do SQL Server |
| `#{SQLServer Username}` | Usuário de conexão |
| `#{SQLServer Password}` | Senha (sensitive) |
| `#{SQLServer JDBC Driver}` | Path do JAR do driver |
| `#{Destination Database}` | Database destino no Snowflake |
| `#{Snowflake Warehouse}` | Warehouse para processamento |
| `#{Snowflake Role}` | Role com acesso ao destino |
| `#{Snowflake Authentication Strategy}` | `SNOWFLAKE_MANAGED` |
| `#{Concurrent Select Queries For Snapshot}` | Queries paralelas no snapshot |
| `#{Concurrent Select Queries For Incremental}` | Queries paralelas no incremental |
| `#{Column Filter JSON}` | JSON com filtro de colunas por tabela |

### Pré-requisitos no SQL Server

```sql
-- 1. Habilitar Change Tracking no database
ALTER DATABASE <database_name>
  SET CHANGE_TRACKING = ON
  (CHANGE_RETENTION = 3 DAYS, AUTO_CLEANUP = ON);

-- 2. Habilitar Change Tracking nas tabelas
ALTER TABLE <schema>.<table> ENABLE CHANGE_TRACKING;

-- 3. Permissões do usuário
GRANT SELECT ON <schema>.<table> TO <user>;
GRANT VIEW CHANGE TRACKING ON <schema>.<table> TO <user>;
```

---

## Padrão de Nomenclatura de Tabelas no Snowflake

| Tipo | Pattern | Exemplo |
|------|---------|---------|
| STAGE (ingestão bruta) | `DB_HML_STAGE.STAGE.{HOST}_{DB}_{SCHEMA}_{TABLE}` | `DB_HML_STAGE.STAGE.SQLSERVER_NORTHWIND_DBO_ORDERS` |
| RAW (FULL) | `{TARGET_DB}.{TARGET_SCHEMA}.{HOST}_{DB}_DBO_{TABLE}` | `NORTHWIND.BRONZE.ORDERS` |
| RAW (INCREMENTAL) | `{RAW_DB}.{SCHEMA}.BD_PROD_{DB}_DBO_{TABLE}` | `RAW.STAGE.BD_PROD_NORTHWIND_DBO_ORDERS` |
| Stream | `DB_HML_STAGE.STAGE.STREAM_{HOST}_{DB}_{SCHEMA}_{TABLE}` | `DB_HML_STAGE.STAGE.STREAM_SQLSERVER_NORTHWIND_DBO_ORDERS` |
| Transient | `DB_HML_STAGE.STAGE.TT_{HOST}_{DB}_{SCHEMA}_{TABLE}` | `DB_HML_STAGE.STAGE.TT_SQLSERVER_NORTHWIND_DBO_ORDERS` |

---

## Como Adicionar uma Nova Tabela

### Para FULL Load

1. Inserir na `FULL_LOAD`:
```sql
INSERT INTO DB_HML_STAGE.SETTINGS.FULL_LOAD VALUES (
    'SQLSERVER',                        -- SOURCE_HOST
    'DATABASE',                         -- SOURCE_DATABASE
    'dbo',                              -- SOURCE_SCHEMA
    'MinhaTabela',                      -- SOURCE_TABLE
    'NORTHWIND',                        -- TARGET_DATABASE
    'BRONZE',                           -- TARGET_SCHEMA
    'MINHATABELA',                      -- TARGET_TABLE
    TRUE,                               -- IS_ACTIVE
    'FULL',                             -- LOAD_TYPE
    CURRENT_TIMESTAMP(),                -- CREATED_AT
    NULL, NULL, NULL,                   -- ROW_COUNT, LAST_DT_LOAD, DURATION
    NULL, NULL, NULL,                   -- COLUMN_NAME_INCREMENT, COLUMN_VALUE, COLUMN_ID
    NULL, NULL                          -- RAW_DB, COLUMN_VALUE_MAX
);
```

2. Criar tabela destino no Snowflake (ou deixar o Openflow criar via PutDatabaseRecord com `Create Table = true`).

3. Executar o Trigger Pipeline manualmente ou aguardar o CRON.

### Para INCREMENTAL Load

1. Inserir na `FULL_LOAD`:
```sql
INSERT INTO DB_HML_STAGE.SETTINGS.FULL_LOAD VALUES (
    'SQLSERVER',                        -- SOURCE_HOST
    'DATABASE',                         -- SOURCE_DATABASE
    'dbo',                              -- SOURCE_SCHEMA
    'MinhaTabela',                      -- SOURCE_TABLE
    'RAW',                              -- TARGET_DATABASE
    'STAGE',                            -- TARGET_SCHEMA
    'MINHATABELA',                      -- TARGET_TABLE
    TRUE,                               -- IS_ACTIVE
    'INCREMENTAL',                      -- LOAD_TYPE
    CURRENT_TIMESTAMP(),                -- CREATED_AT
    NULL, NULL, NULL,                   -- ROW_COUNT, LAST_DT_LOAD, DURATION
    'UpdatedAt',                        -- COLUMN_NAME_INCREMENT (coluna watermark)
    '2024-01-01 00:00:00',              -- COLUMN_VALUE (valor inicial)
    'Id',                               -- COLUMN_ID (PK para MERGE)
    'RAW',                              -- RAW_DB
    NULL                                -- COLUMN_VALUE_MAX
);
```

2. Criar tabela STAGE no Snowflake + Stream + Transient table (para SP_INCREMENTAL_LOAD).

3. Executar o Trigger Pipeline manualmente ou aguardar o CRON.

---

## Checklist Operacional

- [ ] FULL_LOAD populada com tabelas ativas
- [ ] VW_FULL_LOAD gerando SQL correto
- [ ] Controller Services habilitados (estado ENABLED, ícone verde)
- [ ] JDBC Driver (`mssql-jdbc-12.10.0.jre11.jar`) uploaded como asset
- [ ] Parameter Context com `Password` e `Warehouse` configurados
- [ ] Tabelas destino criadas no Snowflake (ou PutDatabaseRecord com auto-create)
- [ ] Network: EAI configurado para acesso ao SQL Server (SPCS)
- [ ] Para CDC: Change Tracking habilitado no SQL Server
