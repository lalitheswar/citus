--
-- MULTI_SINGLE_HASH_REPARTITION_JOIN
--

CREATE SCHEMA single_hash_repartition;

SET search_path TO 'single_hash_repartition';

CREATE TABLE single_hash_repartition_first (id int, sum int, avg float);
CREATE TABLE single_hash_repartition_second (id int, sum int, avg float);
CREATE TABLE ref_table (id int, sum int, avg float);

SET citus.shard_count TO 4;

SELECT create_distributed_table('single_hash_repartition_first', 'id');
SELECT create_distributed_table('single_hash_repartition_second', 'id');
SELECT create_reference_table('ref_table');

SET citus.log_multi_join_order TO ON;
SET client_min_messages TO DEBUG2;

-- a very basic single hash re-partitioning example
EXPLAIN SELECT 
	count(*) 
FROM
	single_hash_repartition_first t1, single_hash_repartition_second t2
WHERE
	t1.id = t2.sum;

-- the same query with the orders of the tables have changed
EXPLAIN SELECT 
	count(*) 
FROM
	single_hash_repartition_second t1, single_hash_repartition_first t2
WHERE
	t2.sum = t1.id;

-- single hash repartition after bcast joins
EXPLAIN SELECT
	count(*)
FROM
	ref_table r1, single_hash_repartition_second t1, single_hash_repartition_first t2
WHERE
	r1.id = t1.id AND t2.sum = t1.id;

-- a more complicated join order, first colocated join, later single hash repartition join
EXPLAIN SELECT 
	count(*) 
FROM
	single_hash_repartition_first t1, single_hash_repartition_first t2, single_hash_repartition_second t3
WHERE
	t1.id = t2.id AND t1.sum = t3.id;


-- a more complicated join order, first hash-repartition join, later single hash repartition join
EXPLAIN SELECT 
	count(*) 
FROM
	single_hash_repartition_first t1, single_hash_repartition_first t2, single_hash_repartition_second t3
WHERE
	t1.sum = t2.sum AND t1.sum = t3.id;

-- single hash repartitioning is not supported between different column types
EXPLAIN SELECT 
	count(*) 
FROM
	single_hash_repartition_first t1, single_hash_repartition_first t2, single_hash_repartition_second t3
WHERE
	t1.id = t2.id AND t1.avg = t3.id;

RESET client_min_messages;

RESET search_path;
DROP SCHEMA single_hash_repartition CASCADE;
