select
    o.amazon_order_id as "Order ID"
    , o.purchase_date as "Order Timestamp"
    , o.number_of_items_shipped + number_of_items_unshipped as "Order Quantity"
    , o.order_total_amount as "Order Revenue"
    , oip.PROMOTION_ID as "promotion-ids"
    , buyer_info_buyer_email as "Email"
    , shipping_address_city as "City"
    , shipping_address_state_or_region as "State"
    , shipping_address_postal_code as "Zip Code"
    from  MARKETING_DATABASE.AMAZON_SELLING_PARTNER.ORDERS o
left join MARKETING_DATABASE.AMAZON_SELLING_PARTNER.ORDER_ITEM as oi
on o.amazon_order_id = oi.amazon_order_id
left join MARKETING_DATABASE.AMAZON_SELLING_PARTNER.ORDER_ITEM_PROMOTION_ID as oip
on o.amazon_order_id = oip.amazon_order_id 

where shipping_address_country_code = 'US'