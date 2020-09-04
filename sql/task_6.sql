-- What is the average amount of money
-- spent per session BETWEEN '20170801' AND '20180801'.

-- todo: how to know how much money has been spent?
with base as (
    select
        fullVisitorId,
        sum(totals.transactionRevenue) as revenue
    from gridu.sessions
    where
        totals.transactions > 0 and
        totals.visits > 0 and
        date between '20170801' and '20180801'
    group by fullVisitorId
)
select
    avg(revenue) as avg_revenue
from base