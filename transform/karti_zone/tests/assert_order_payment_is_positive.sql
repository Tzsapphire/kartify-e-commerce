select 
    order_id,
    sum(total_price) as order_cost
from {{ref('stg_order_items')}} 
group by order_id
having sum(total_price) < 0

-- select 
--     order_id,
--     sum(soi.total_price ) as order_cost
-- from stg_order_items soi 
-- group by order_id
-- having sum(soi.total_price ) < 0
-- order by order_id ;