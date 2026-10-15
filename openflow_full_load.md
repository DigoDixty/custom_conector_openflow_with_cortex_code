# Openflow Full Load — PostgreSQL → Snowflake

## Arquitetura do Flow

```
ListDatabaseTables → ExecuteSQLRecord → PutSnowpipeStreaming
```

1. **ListDatabaseTables** — Descobre todas as tabelas do schema `public` no PostgreSQL
2. **ExecuteSQLRecord** — Executa `SELECT * FROM ${db.table.schema}.${db.table.name}` para cada tabela
3. **PutSnowpipeStreaming** — Escreve os dados no Snowflake via Snowpipe Streaming

## Objetos Criados no Snowflake

| Objeto | Nome | Detalhes |
|--------|------|----------|
| Role | `OPENFLOW_INGEST_ROLE` | Role para ingestão, com grants em OPENFLOW_DB |
| User | `OPENFLOW_INGEST_USER` | Usuário de serviço, default role/warehouse configurados |
| Warehouse | `OPENFLOW_INGEST_WH` | XSMALL, single cluster, autosuspend 60s, standard |
| Database | `OPENFLOW_DB` | Database destino para dados ingeridos |
| Network Rule | `OPENFLOW_DB.PUBLIC.POSTGRES_OPENFLOW_NETWORK_RULE` | Egress para 169.58.185.207:5432 |
| EAI | `POSTGRES_OPENFLOW_EAI` | External Access Integration para PostgreSQL |
| Deployment | `OPENFLOW_DEPLOY` | Snowflake deployment (pré-existente) |
| Runtime | `OPENFLOW_DB.PUBLIC.OPENFLOW_INGEST_RT` | Small S1 (1 CPU, 4GB), EAI anexada |

## Objetos no Canvas NiFi (Runtime)

### Process Group: "PostgreSQL Full Load"
- **ID:** `0a4f65c4-01a1-1000-ffff-ffffffa3bf71`
- **Parameter Context:** `PG Full Load Params`

### Controller Services

| Service | Tipo | ID |
|---------|------|----|
| PostgreSQL JDBC Pool | DBCPConnectionPool | `0a511ece-01a1-1000-0000-00007b7216e2` |
| JSON Writer | JsonRecordSetWriter | `0a511fa0-01a1-1000-0000-000027dbf0c4` |
| JSON Reader | JsonTreeReader | `0a553345-01a1-1000-0000-000039763a27` |

### Processors

| Processor | Tipo | ID | Configuração Chave |
|-----------|------|----|-------------------|
| List PostgreSQL Tables | ListDatabaseTables | `0a549fa8-01a1-1000-0000-00000be569cc` | Schema: public, Interval: 1h |
| Read Table Data | ExecuteSQLRecord | `0a54a0a6-01a1-1000-ffff-ffff9c1e07e0` | SQL dinâmico via atributos FlowFile |
| Write to Snowflake | PutSnowpipeStreaming | `0a54a19a-01a1-1000-ffff-ffffc6a987e8` | SNOWFLAKE_MANAGED auth, tabela dinâmica |

### Parameter Context: "PG Full Load Params"

| Parâmetro | Valor | Sensível |
|-----------|-------|----------|
| pg.jdbc.url | `jdbc:postgresql://169.58.185.207:5432/pg_sf_mirror_db` | Não |
| pg.username | `usr_sf_mirror_ro` | Não |
| pg.password | (configurado) | Sim |
| sf.database | `OPENFLOW_DB` | Não |
| sf.schema | `PUBLIC` | Não |
| pg.driver.jar | Asset: postgresql-42.7.4.jar | Não |

## Fluxo de Dados

```
PostgreSQL (169.58.185.207:5432)
  └── pg_sf_mirror_db.public.*
       │
       ▼
  [ListDatabaseTables] — descobre tabelas (a cada 1h)
       │ FlowFile com atributos: db.table.name, db.table.schema
       ▼
  [ExecuteSQLRecord] — SELECT * FROM cada tabela
       │ FlowFile com registros JSON
       ▼
  [PutSnowpipeStreaming] — escreve em OPENFLOW_DB.PUBLIC.<table_name>
       │ Auth: SNOWFLAKE_MANAGED (role: OPENFLOW_INGEST_ROLE)
       ▼
  Snowflake (OPENFLOW_DB.PUBLIC)
```

## Comandos Úteis (nipyapi)

```bash
export PATH="$PATH:/Users/dixty/Library/Python/3.9/bin"

# Status do flow
nipyapi --profile triggo_openflow_ingest_rt ci get_status --process_group_id "0a4f65c4-01a1-1000-ffff-ffffffa3bf71"

# Iniciar flow
nipyapi --profile triggo_openflow_ingest_rt ci start_flow --process_group_id "0a4f65c4-01a1-1000-ffff-ffffffa3bf71"

# Parar flow
nipyapi --profile triggo_openflow_ingest_rt ci stop_flow --process_group_id "0a4f65c4-01a1-1000-ffff-ffffffa3bf71"

# Ver erros
python3 -c "
import nipyapi; nipyapi.profiles.switch('triggo_openflow_ingest_rt')
for b in nipyapi.bulletins.get_bulletin_board(pg_id='0a4f65c4-01a1-1000-ffff-ffffffa3bf71'):
    print(f'[{b.level}] {b.source_name}: {b.message}')
"
```

