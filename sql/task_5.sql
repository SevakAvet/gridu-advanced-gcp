-- What was the average total transactions per user
-- that made a purchase BETWEEN '20170801' AND '20180801'.

select
    avg(totals.transactions) as avg_transactions
from gridu.sessions
where
    totals.transactions > 0 and
    date between '20170801' and '20180801'