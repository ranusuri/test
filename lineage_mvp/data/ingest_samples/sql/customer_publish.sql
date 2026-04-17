INSERT INTO customer_publish_ready
SELECT c.customer_id,
       c.segment,
       o.last_order_amount
FROM customer_raw_bridge c
JOIN orders_reference o
  ON c.customer_id = o.customer_id;
