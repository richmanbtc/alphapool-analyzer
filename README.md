
dashboard

- portfolio pnl
- model pnl
- portfolio weights (var: model_id)
- model positions (var: model_id)

tables

- analyzer_positions (tournament, model_id, timestamp, symbol, position)
- analyzer_rets (tournament, model_id, timestamp, ret)

http://localhost:3000/dashboard/script/alphapool.js

Cloud SQL storage rapidly grows

- VACUUM before process
- Maybe we could try to update only the most recent data.
- VACUUM FULL and DROP TABLE did not reduce storage on the Cloud SQL web
- Since autovacuum seems to be working, autovacuum alone does not prevent storage increase
- https://recruit.gmo.jp/engineer/jisedai/blog/cloud-sql-storage/
- disabling point in time recovery reduced storage
- Not sure if VACUUM, VACUUM FULL, or DROP TABLE is sufficient when point in recovery is disabled

timeout

- It appears to be taking a long time to insert analyzer_positions.

postgresql auto increment error

- https://stackoverflow.com/questions/56533378/postgresql-set-max-value-for-sequence-to-a-higher-value
- Modified code to use bigint. If the table has already been created, it can be modified with the following sql

```sql
ALTER TABLE table_name ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE table_name_id_seq AS bigint MAXVALUE 9223372036854775807;
```
