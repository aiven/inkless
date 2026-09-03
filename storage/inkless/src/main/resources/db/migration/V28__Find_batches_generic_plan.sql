-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
-- Reuse a generic plan for `find_batches_v2`'s per-partition query.
--
-- The function executes its inner `batches`-to-`files` query once per requested partition. PostgreSQL's
-- automatic plan cache can keep choosing a custom plan for that query, which invokes the planner for
-- every partition even though each custom plan has the same index-scan shape. At normal fetch rates,
-- this turns hundreds of function calls into thousands of planning operations per second.
--
-- The function-level setting makes PostgreSQL build and reuse one generic plan per backend. It applies
-- only while `find_batches_v2` runs, so it does not change planning for other control-plane queries. The
-- generic plan retains the parameterized index scan that lets the function stop at the fetch budget.
ALTER FUNCTION find_batches_v2(find_batches_request_v1[], INT, INT)
    SET plan_cache_mode = force_generic_plan;
