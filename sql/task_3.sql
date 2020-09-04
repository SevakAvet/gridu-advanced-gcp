-- What was the average number of product pageviews for users
-- who made a purchase BETWEEN '20170801' AND '20180801'.
-- purchase when totals.transactions >=1.

select
    avg(totals.pageviews) as avg_pageviews
from gridu.sessions
where
    totals.transactions > 0 and
    date between '20170801' and '20180801'