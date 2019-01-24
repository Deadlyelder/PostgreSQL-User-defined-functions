drop function inlineMax(anyelement, anyelement) cascade;
drop function inlineMin(anyelement, anyelement) cascade;

create or replace function inlineMax(anyelement, anyelement)
returns anyelement
as
$$
select case when $1 > $2 then $1 else coalesce($2, $1) end 
$$ language 'sql' volatile;

create or replace function inlineMin(anyelement, anyelement)
returns anyelement
as
$$
select case when $1 < $2 then $1 else coalesce($2, $1) end 
$$ language 'sql' volatile;
