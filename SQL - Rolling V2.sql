drop aggregate if exists rolling_avg_with_offset(numeric, date, interval, interval);
drop aggregate if exists rolling_avg(numeric, date, interval);
drop aggregate if exists rolling_stddev_with_offset(numeric, date, interval, interval);
drop aggregate if exists rolling_stddev(numeric, date, interval);
drop aggregate if exists rolling_sum_with_offset(numeric, date, interval, interval);
drop aggregate if exists rolling_sum(numeric, date, interval);
drop aggregate if exists rolling_count_with_offset(numeric, date, interval, interval);
drop aggregate if exists rolling_count(numeric, date, interval);
drop aggregate if exists rolling_median_with_offset(numeric, date, interval, interval);
drop aggregate if exists rolling_median(numeric, date, interval);

drop function if exists array_append_with_offset(paire_array_num_array_date, numeric, date, interval, interval);
drop function if exists array_append_without_offset(paire_array_num_array_date, numeric, date, interval);

drop function if exists avg_paire_array_num_array_date(paire_array_num_array_date);
drop function if exists stddev_paire_array_num_array_date(paire_array_num_array_date);
drop function if exists sum_paire_array_num_array_date(paire_array_num_array_date);
drop function if exists count_paire_array_num_array_date(paire_array_num_array_date);
drop function if exists median_paire_array_num_array_date(paire_array_num_array_date);

drop type if exists paire_array_num_array_date;

create type paire_array_num_array_date as (num numeric[], dt date[], curdt date, iv interval, os interval);

create function array_append_with_offset(paire_array_num_array_date, numeric, date, interval, interval)
returns paire_array_num_array_date as $$
select (array_append(($1).num, $2), array_append(($1).dt, $3), $3, $4, $5)::paire_array_num_array_date;
$$ language sql;

create function array_append_without_offset(paire_array_num_array_date, numeric, date, interval)
returns paire_array_num_array_date as $$
select (array_append(($1).num, $2), array_append(($1).dt, $3), $3, $4, interval '0 sec')::paire_array_num_array_date;
$$ language sql;

create function avg_paire_array_num_array_date(paire_array_num_array_date)
returns numeric as $$
select avg("MT") from unnest(($1).dt, ($1).num) f("DT", "MT") where "DT" > ($1).curdt - ($1).os - ($1).iv and "DT" <= ($1).curdt - ($1).os;
$$ language sql;

create aggregate rolling_avg_with_offset(numeric, date, interval, interval)
(
    sfunc = array_append_with_offset,
    stype = paire_array_num_array_date,
    --initcond = '({},{},0)'::paire_array_num_array_date,
    finalfunc = avg_paire_array_num_array_date
);

create aggregate rolling_avg(numeric, date, interval)
(
    sfunc = array_append_without_offset,
    stype = paire_array_num_array_date,
    finalfunc = avg_paire_array_num_array_date
);

create function stddev_paire_array_num_array_date(paire_array_num_array_date)
returns numeric as $$
select stddev("MT") from unnest(($1).dt, ($1).num) f("DT", "MT") where "DT" > ($1).curdt - ($1).os - ($1).iv and "DT" <= ($1).curdt - ($1).os;
$$ language sql;

create aggregate rolling_stddev_with_offset(numeric, date, interval, interval)
(
    sfunc = array_append_with_offset,
    stype = paire_array_num_array_date,
    --initcond = '({},{},0)'::paire_array_num_array_date,
    finalfunc = stddev_paire_array_num_array_date
);

create aggregate rolling_stddev(numeric, date, interval)
(
    sfunc = array_append_without_offset,
    stype = paire_array_num_array_date,
    finalfunc = stddev_paire_array_num_array_date
);

create function sum_paire_array_num_array_date(paire_array_num_array_date)
returns numeric as $$
select sum("MT") from unnest(($1).dt, ($1).num) f("DT", "MT") where "DT" > ($1).curdt - ($1).os - ($1).iv and "DT" <= ($1).curdt - ($1).os;
$$ language sql;

create aggregate rolling_sum_with_offset(numeric, date, interval, interval)
(
    sfunc = array_append_with_offset,
    stype = paire_array_num_array_date,
    --initcond = '({},{},0)'::paire_array_num_array_date,
    finalfunc = sum_paire_array_num_array_date
);

create aggregate rolling_sum(numeric, date, interval)
(
    sfunc = array_append_without_offset,
    stype = paire_array_num_array_date,
    finalfunc = sum_paire_array_num_array_date
);

create function count_paire_array_num_array_date(paire_array_num_array_date)
returns bigint as $$
select count(*) from unnest(($1).dt, ($1).num) f("DT", "MT") where "DT" > ($1).curdt - ($1).os - ($1).iv and "DT" <= ($1).curdt - ($1).os;
$$ language sql;

create aggregate rolling_count_with_offset(numeric, date, interval, interval)
(
    sfunc = array_append_with_offset,
    stype = paire_array_num_array_date,
    --initcond = '({},{},0)'::paire_array_num_array_date,
    finalfunc = count_paire_array_num_array_date
);

create aggregate rolling_count(numeric, date, interval)
(
    sfunc = array_append_without_offset,
    stype = paire_array_num_array_date,
    finalfunc = count_paire_array_num_array_date
);

create function median_paire_array_num_array_date(paire_array_num_array_date)
returns numeric as $$
select med_median("MT"::int) from unnest(($1).dt, ($1).num) f("DT", "MT") where "DT" > ($1).curdt - ($1).os - ($1).iv and "DT" <= ($1).curdt - ($1).os;
$$ language sql;

create aggregate rolling_median_with_offset(numeric, date, interval, interval)
(
    sfunc = array_append_with_offset,
    stype = paire_array_num_array_date,
    --initcond = '({},{},0)'::paire_array_num_array_date,
    finalfunc = median_paire_array_num_array_date
);

create aggregate rolling_median(numeric, date, interval)
(
    sfunc = array_append_without_offset,
    stype = paire_array_num_array_date,
    finalfunc = median_paire_array_num_array_date
);


drop aggregate if exists rolling_percentile_with_offset(numeric, date, interval, interval, double precision);
drop aggregate if exists rolling_percentile(numeric, date, interval, double precision);

drop function if exists percentile_disc_paire_array_num_array_date_q(paire_array_num_array_date_q);

drop function if exists array_append_with_offset_q(paire_array_num_array_date_q, numeric, date, interval, interval, double precision);
drop function if exists array_append_without_offset_q(paire_array_num_array_date_q, numeric, date, interval, double precision);

drop type if exists paire_array_num_array_date_q;

create type paire_array_num_array_date_q as (num numeric[], dt date[], curdt date, iv interval, os interval, q double precision);

create function array_append_with_offset_q(paire_array_num_array_date_q, numeric, date, interval, interval, double precision)
returns paire_array_num_array_date_q as $$
select (array_append(($1).num, $2), array_append(($1).dt, $3), $3, $4, $5, $6)::paire_array_num_array_date_q;
$$ language sql;

create function array_append_without_offset_q(paire_array_num_array_date_q, numeric, date, interval, double precision)
returns paire_array_num_array_date_q as $$
select (array_append(($1).num, $2), array_append(($1).dt, $3), $3, $4, interval '0 sec', $5)::paire_array_num_array_date_q;
$$ language sql;

create function percentile_disc_paire_array_num_array_date_q(paire_array_num_array_date_q)
returns numeric as $$
select percentile_disc(($1).q) within group (order by "MT" asc) from unnest(($1).dt, ($1).num) f("DT", "MT") where "DT" > ($1).curdt - ($1).os - ($1).iv and "DT" <= ($1).curdt - ($1).os;
$$ language sql;

create aggregate rolling_percentile_with_offset(numeric, date, interval, interval, double precision)
(
    sfunc = array_append_with_offset_q,
    stype = paire_array_num_array_date_q,
    --initcond = '({},{},0)'::paire_array_num_array_date,
    finalfunc = percentile_disc_paire_array_num_array_date_q
);

create aggregate rolling_percentile(numeric, date, interval, double precision)
(
    sfunc = array_append_without_offset_q,
    stype = paire_array_num_array_date_q,
    finalfunc = percentile_disc_paire_array_num_array_date_q
);
