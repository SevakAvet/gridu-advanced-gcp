-- Find Products purchased by customers who purchased product A?
-- product A : hits.product.productSKU = 'GGOEGAEC033115'
-- purchased : hits.eCommerceAction.action_type = '6'

with all_hits as (
  select
    fullVisitorId,
    hits
  from gridu.sessions, unnest(hits) as hits
),
all_products as (
  select
    fullVisitorId,
    products
  from all_hits, unnest(hits.product) as products
),
users_who_bought_sku as (
  select
    fullVisitorId
  from all_products
    where products.productSKU = '9182785'
),
products_bought_by_users as (
  select
    all_hits.hits.product
  from all_hits
  join users_who_bought_sku using (fullVisitorId)
    where hits.eCommerceAction.action_type = '6'
)
select distinct
  productSKU
from products_bought_by_users, unnest(product)