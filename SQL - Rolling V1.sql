drop aggregate if exists rolling_avg(paire_num_date, interval);
drop aggregate if exists rolling_stddev(paire_num_date, interval);
drop aggregate if exists rolling_sum(paire_num_date, interval);
drop aggregate if exists rolling_med_median(paire_num_date, interval);
drop aggregate if exists rolling_count(paire_num_date, interval);
drop aggregate if exists rolling_percentile_disc(paire_num_date, interval, double precision);

drop function if exists array_append_filter(paire_num_date[], paire_num_date, interval);
drop function if exists array_append_filter2(paire_num_date[], paire_num_date, interval, double precision);

drop function if exists avg_paire_num_date(paire_num_date[]);
drop function if exists stddev_paire_num_date(paire_num_date[]);
drop function if exists med_median_paire_num_date(paire_num_date[]);
drop function if exists count_paire_num_date(paire_num_date[]);
drop function if exists sum_paire_num_date(paire_num_date[]);
drop function if exists percentile_disc_paire_num_date(paire_num_date[], double precision);

drop type if exists paire_num_date;

create type paire_num_date as (num numeric, dt date);

create function array_append_filter(paire_num_date[], paire_num_date, interval)
returns paire_num_date[] as $$
select array_agg(("MT", "DT")::paire_num_date) from unnest(array_append($1, $2)) f("MT", "DT") where "DT" > ($2).dt - $3;
$$ language sql;

create function array_append_filter2(paire_num_date[], paire_num_date, interval, double precision)
returns paire_num_date[] as $$
select array_agg(("MT", "DT")::paire_num_date) from unnest(array_append($1, $2)) f("MT", "DT") where "DT" > ($2).dt - $3;
$$ language sql;

create function avg_paire_num_date(paire_num_date[])
returns numeric as $$
select avg("MT") from unnest($1) f("MT", "DT");
$$ language sql;

create function stddev_paire_num_date(paire_num_date[])
returns numeric as $$
select stddev("MT") from unnest($1) f("MT", "DT");
$$ language sql;

create function med_median_paire_num_date(paire_num_date[])
returns numeric as $$
select med_median("MT"::int) from unnest($1) f("MT", "DT");
$$ language sql;

create function sum_paire_num_date(paire_num_date[])
returns numeric as $$
select sum("MT") from unnest($1) f("MT", "DT");
$$ language sql;

create function count_paire_num_date(paire_num_date[])
returns bigint as $$
select count(*) from unnest($1) f("MT", "DT");
$$ language sql;

create function percentile_disc_paire_num_date(paire_num_date[], double precision)
returns numeric as $$
select percentile_disc($2) within group (order by "MT") from unnest($1) f("MT", "DT");
$$ language sql;

create aggregate rolling_avg(paire_num_date, interval)
(
    sfunc = array_append_filter,
    stype = paire_num_date[],
    initcond = '{}',
    finalfunc = avg_paire_num_date
);

create aggregate rolling_stddev(paire_num_date, interval)
(
    sfunc = array_append_filter,
    stype = paire_num_date[],
    initcond = '{}',
    finalfunc = stddev_paire_num_date
);

create aggregate rolling_med_median(paire_num_date, interval)
(
    sfunc = array_append_filter,
    stype = paire_num_date[],
    initcond = '{}',
    finalfunc = med_median_paire_num_date
);

create aggregate rolling_sum(paire_num_date, interval)
(
    sfunc = array_append_filter,
    stype = paire_num_date[],
    initcond = '{}',
    finalfunc = sum_paire_num_date
);

create aggregate rolling_count(paire_num_date, interval)
(
    sfunc = array_append_filter,
    stype = paire_num_date[],
    initcond = '{}',
    finalfunc = count_paire_num_date
);
/*
create aggregate rolling_percentile_disc(paire_num_date, interval, double precision)
(
    sfunc = array_append_filter2,
    --stype = (paire_num_date[],double precision),
    initcond = '{}',
    finalfunc = percentile_disc_paire_num_date
);*/