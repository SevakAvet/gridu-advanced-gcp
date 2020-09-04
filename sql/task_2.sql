-- What was the real bounce rate per traffic source for  youtube.com, facebook.com
-- BETWEEN '20170801' AND '20180801'.
-- The real bounce rate is defined as the percentage of visits with a single pageview.


select trafficSource.source,
       count(*)                                            as total_count,
       countif(totals.pageviews = 1)                       as single_pageviews,
       countif(totals.pageviews = 1) / count(*)            as single_pageview_rate
from gridu.sessions
where trafficSource.source in ('youtube.com', 'facebook.com')
  and date between '20170801' AND '20180801'
group by trafficSource.source