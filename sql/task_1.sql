-- What is the total number of transactions generated per device.browser
-- date BETWEEN '20170801' AND '20180801' for browsers Chrome, Safari, Firefox


select
    device.browser,
    sum(totals.transactions) as total,
from gridu.sessions
where
    date between '20170801' AND '20180801'
    and device.browser in ('Chrome', 'Safari', 'Firefox')
group by device.browser