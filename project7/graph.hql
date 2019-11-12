drop table Graph;

create table Graph(
    node int,
    link int)
row format delimited fields terminated by "," stored as textfile;

LOAD DATA INPATH '${hiveconf:G}' INTO TABLE Graph;

--select node, link from Graph;
select link, count(node) as total_link from Graph group by link order by total_link desc;
