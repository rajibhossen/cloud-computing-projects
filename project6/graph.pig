data = LOAD '$G' USING PigStorage(',') as (id: int, link:int);
by_id = GROUP data BY link;
count = FOREACH by_id GENERATE group as id, COUNT($1) as total;
order_by = ORDER count BY total DESC;
STORE order_by INTO '$O' USING PigStorage(',');

