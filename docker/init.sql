CREATE EXTENSION IF NOT EXISTS citus;
-- Add workers to coordinator
SELECT
  CASE
    WHEN to_regproc('citus_add_node') IS NOT NULL THEN citus_add_node('worker1', 5432)
    ELSE master_add_node('worker1', 5432)
  END;
SELECT
  CASE
    WHEN to_regproc('citus_add_node') IS NOT NULL THEN citus_add_node('worker2', 5432)
    ELSE master_add_node('worker2', 5432)
  END;
