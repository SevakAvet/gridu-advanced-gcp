-- What is the first 4 sequence of pages
-- viewed BETWEEN '20170801' AND '20180801'?
-- hits.type = 'page' for every record

with all_pages as (
  select
    s.date as date,
    s.fullVisitorId,
    ARRAY(select x from unnest(s.hits) as x where x.type = 'PAGE') as hits
  from gridu.sessions s
)
select
  date,
  fullVisitorId,
  hit.page.pagePath as path
from all_pages p, unnest(hits) as hit
order by date
limit 4