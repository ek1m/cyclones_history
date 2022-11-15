BEGIN;

delete from tmp.cyclones_history
where date_from = '{date_load}'
;

update tmp.cyclones_history
set date_end = null
where date_end = '{before_date_load}'
;

update tmp.cyclones_history
set date_end = '{before_date_load}'
from tmp.cyclones_tmp
where NOT(cyclones_tmp.id = cyclones_history.id and cyclones_tmp.status = cyclones_history.status) and date_end is null
;

delete from tmp.cyclones_tmp
using tmp.cyclones_history
where cyclones_tmp.id = cyclones_history.id and cyclones_history.date_end != '{before_date_load}'
;

insert into tmp.cyclones_history (date_from, id, status)
select cyclones_tmp.date, cyclones_tmp.id, cyclones_tmp.status from  tmp.cyclones_tmp
;

truncate table tmp.cyclones_tmp;

COMMIT;
