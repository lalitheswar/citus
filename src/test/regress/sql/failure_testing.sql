-- disable the maintenance daemon, prevent it from making any connections
ALTER SYSTEM SET citus.distributed_deadlock_detection_factor TO -1;
ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
ALTER SYSTEM set citus.enable_statistics_collection TO false;
SELECT pg_reload_conf();

SELECT citus.mitmproxy('flow.allow()');

-- add the workers
SELECT master_add_node('localhost', :worker_1_port);  -- the second worker
SELECT master_add_node('localhost', :worker_2_port + 2);  -- the first worker, behind a mitmproxy

CREATE TABLE test (a int, b int);
SELECT create_distributed_table('test', 'a');

BEGIN;
UPDATE test SET b = 2 WHERE a = 2;
ROLLBACK;

SELECT citus.mitmproxy('flow.contains(b"UPDATE").kill()');

BEGIN;
UPDATE test SET b = 2 WHERE a = 2;
ROLLBACK;

SELECT citus.mitmproxy('flow.contains(b"BEGIN").kill()');

BEGIN;
UPDATE test SET b = 2 WHERE a = 2;
ROLLBACK;

SET citus.multi_shard_commit_protocol = '2pc';
SELECT citus.mitmproxy('flow.contains(b"COMMIT").kill()');

BEGIN;
UPDATE test SET b = 2 WHERE a = 2;
COMMIT;

SELECT citus.mitmproxy('flow.allow()');

SET citus.multi_shard_commit_protocol = '2pc';

BEGIN;
UPDATE test SET b = 2 WHERE a = 2;
COMMIT;
