| Parâmetro | Valor (Admin / DDL / DML) | Valor (Read-Only / SELECT) |
| :--- | :--- | :--- |
| **Host (Direto TCP / IP VPS)** | `[IP_ADDRESS]` (porta `5432`) | `[IP_ADDRESS]` (porta `5432`) |
| **Host (Interno Docker)** | `office-postgres` (porta `5432`) | `office-postgres` (porta `5432`) |
| **Host (Local/VPS Host)** | `localhost:5432` | `localhost:5432` |
| **Database** | `pg_sf_mirror_db` | `pg_sf_mirror_db` |
| **User (Usuário)** | `usr_sf_mirror_ro` |
| **Schema Padrão** | `public` | `public` |
| **Privilégios** | Leitura, Escrita, DDL e DML | **Apenas SELECT (Read-Only)** |
| **String de Conexão (URI)** | `postgresql://usr_sf_mirror_ro:SfMirror_ReadOnly_2026!Key@[IP_ADDRESS]/pg_sf_mirror_db` |