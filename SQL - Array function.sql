CREATE OR REPLACE FUNCTION array_avg(double precision[])
RETURNS double precision AS $$
SELECT avg(v) FROM unnest($1) g(v) where v is not null
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION array_stddev(double precision[])
RETURNS double precision AS $$
SELECT stddev(v) FROM unnest($1) g(v) where v is not null
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION array_sum(double precision[])
RETURNS double precision AS $$
SELECT sum(v) FROM unnest($1) g(v) where v is not null
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION array_count(double precision[])
RETURNS bigint AS $$
SELECT count(v) FROM unnest($1) g(v)  where v is not null
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION array_median(double precision[])
RETURNS numeric AS $$
SELECT med_median(v::int) FROM unnest($1) g(v) where v is not null 
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION array_percentile_disc(double precision, double precision[])
RETURNS double precision AS $$
SELECT percentile_disc($1) within group (order by v) FROM unnest($2) g(v) where v is not null 
$$ LANGUAGE sql;