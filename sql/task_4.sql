-- What was the average number of product pageviews for users
-- who did not make a purchase BETWEEN '20170801' AND '20180801'.

select
    avg(totals.pageviews) as avg_pageviews
from gridu.sessions
where
    (totals.transactions is null or totals.transactions = 0)
    and date between '20170801' and '20180801'