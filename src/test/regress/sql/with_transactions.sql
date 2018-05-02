CREATE SCHEMA with_transactions;
SET search_path TO 	with_transactions, public;

SET citus.shard_count TO 4;
SET citus.next_placement_id TO 800000;

CREATE TABLE with_transactions.raw_table (tenant_id int, income float, created_at timestamptz);
SELECT create_distributed_table('raw_table', 'tenant_id');

CREATE TABLE with_transactions.second_raw_table (tenant_id int, income float, created_at timestamptz);
SELECT create_distributed_table('second_raw_table', 'tenant_id');


INSERT INTO 
	raw_table (tenant_id, income, created_at) 
SELECT 
	i % 10, i * 10.0, timestamp '2014-01-10 20:00:00' + i * interval '1 day' 
FROM 
	generate_series (0, 100) i;

INSERT INTO second_raw_table SELECT * FROM raw_table;

SET client_min_messages TO DEBUG1;

-- run a transaction which DELETE 
BEGIN;

	WITH ids_to_delete AS
		(
			SELECT tenant_id FROM raw_table WHERE income < 250
		),
		deleted_ids AS
		(
			DELETE FROM raw_table WHERE created_at  < '2014-02-10 20:00:00' AND tenant_id IN (SELECT * from ids_to_delete) RETURNING tenant_id
		)
		UPDATE raw_table SET income = income * 2 WHERE tenant_id IN (SELECT tenant_id FROM deleted_ids);

ROLLBACK;

-- see that both UPDATE and DELETE commands are rollbacked
SELECT count(*) FROM raw_table;
SELECT max(income) FROM raw_table;

-- multi-statement multi shard modifying statements should work
BEGIN;

	WITH ids_inserted AS
		(
			INSERT INTO raw_table VALUES (11, 1000, now()) RETURNING tenant_id 
		)
		UPDATE raw_table SET created_at = '2001-02-10 20:00:00' WHERE tenant_id IN (SELECT tenant_id FROM ids_inserted);

	TRUNCATE second_raw_table;
COMMIT;

-- sequential insert followed by parallel update causes execution issues
WITH ids_inserted AS
(
  INSERT INTO raw_table VALUES (11, 1000, now()), (12, 1000, now()), (13, 1000, now()) RETURNING tenant_id 
)
UPDATE raw_table SET created_at = '2001-02-10 20:00:00' WHERE tenant_id IN (SELECT tenant_id FROM ids_inserted);

-- make sure that everything committed
SELECT count(*) FROM raw_table;
SELECT count(*) FROM raw_table WHERE created_at = '2001-02-10 20:00:00';
SELECT count(*) FROM second_raw_table;

RESET client_min_messages;
RESET citus.shard_count;
DROP SCHEMA with_transactions CASCADE;
