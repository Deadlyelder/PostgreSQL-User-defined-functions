drop aggregate if exists med_median(numeric);
drop function if exists tmedian(numeric[]);

create function tmedian(numeric[])
returns numeric as $$
select avg(val) 
from 
(
    select val
    from unnest($1) val
    order by 1
    LIMIT  2 - mod(array_upper($1, 1), 2)
    OFFSET ceil(array_upper($1, 1) / 2.0) - 1
   ) "sub";
$$ LANGUAGE 'sql' IMMUTABLE;
 
create aggregate med_median(numeric) 
(
 sfunc = array_append,
 stype = numeric[],
 ffunc = tmedian,
 initcond = '{}'
);