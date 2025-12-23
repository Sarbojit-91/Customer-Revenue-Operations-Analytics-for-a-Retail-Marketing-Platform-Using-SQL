#create database marketing_db;

use marketing_db;

select * from customers;
select * from shipments;
select * from inventory_movements;
select * from order_items;
select * from orders;
select * from products;
select * from returns;
select * from stores;

#Step 0 DATA VALIDATION & INTEGRITY CHECKS
#Schema Sanity (ID Relationships) Before metrics, we confirm referential integrity.

#0.1 Orders without Customers 
SELECT
    o.order_id,
    o.customer_id
FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

#0.2 Orders without Order Items
SELECT
    o.order_id
FROM orders o
LEFT JOIN order_items oi
    ON o.order_id = oi.order_id
WHERE oi.order_id IS NULL;

#0.3 Order Items without Products
SELECT
    oi.order_item_id,
    oi.product_id
FROM order_items oi
LEFT JOIN products p
    ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

#0.4 Orders without Shipments
SELECT
    o.order_id,
    o.status
FROM orders o
LEFT JOIN shipments s
    ON o.order_id = s.order_id
WHERE s.order_id IS NULL
  AND o.status IN ('shipped', 'delivered');
  
  #Revenue & Pricing Consistency Checks
  
  #1.1 Line Amount Validation
  SELECT
    order_item_id,
    unit_price,
    quantity,
    discount,
    line_amount,
    (unit_price * quantity - discount) AS expected_amount
FROM order_items
WHERE ABS(line_amount - (unit_price * quantity - discount)) > 1;

#1.2 Negative or Zero Revenue Lines
SELECT *
FROM order_items
WHERE line_amount <= 0;

#Returns Integrity 

#2.1 Returns without Matching Order Items
SELECT
    r.return_id,
    r.order_item_id
FROM returns r
LEFT JOIN order_items oi
    ON r.order_item_id = oi.order_item_id
WHERE oi.order_item_id IS NULL;

#2.2 Returned Quantity > Purchased Quantity
SELECT
    r.order_item_id,
    oi.quantity AS purchased_qty,
    r.quantity_returned
FROM returns r
JOIN order_items oi
    ON r.order_item_id = oi.order_item_id
WHERE r.quantity_returned > oi.quantity;

##2.3 Refund Amount > Line Amount
SELECT
    r.order_item_id,
    r.refund_amount,
    oi.line_amount
FROM returns r
JOIN order_items oi
    ON r.order_item_id = oi.order_item_id
WHERE r.refund_amount > oi.line_amount;

#Order Status Logic Checks

#3.1 Delivered Orders without Delivered Timestamp
SELECT
    o.order_id,
    o.status,
    s.delivered_at
FROM orders o
JOIN shipments s
    ON o.order_id = s.order_id
WHERE o.status = 'delivered'
  AND s.delivered_at IS NULL;

#3.2 Shipped Before Order Creation 
SELECT
    o.order_id,
    o.order_datetime,
    s.shipped_at
FROM orders o
JOIN shipments s
    ON o.order_id = s.order_id
WHERE s.shipped_at < o.order_datetime;

#Inventory Movement Sanity

#4.1 Unknown Movement Types
SELECT DISTINCT movement_type
FROM inventory_movements;

#4.2 Negative Inbound or Positive Returns
SELECT *
FROM inventory_movements
WHERE (movement_type = 'inbound' AND quantity_delta < 0)
   OR (movement_type = 'return' AND quantity_delta <= 0);


#Step 1 — CORE REVENUE & ORDER KPIs

#Order-Level Revenue CTE 
WITH order_revenue AS (
    SELECT
        o.order_id,
        o.order_datetime,
        o.customer_id,
        o.store_id,
        o.sales_channel,
        o.payment_method,
        SUM(oi.line_amount) AS net_revenue,
        SUM(oi.discount) AS total_discount,
        COUNT(DISTINCT oi.order_item_id) AS total_items
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY
        o.order_id,
        o.order_datetime,
        o.customer_id,
        o.store_id,
        o.sales_channel,
        o.payment_method
)
SELECT *
FROM order_revenue;

#Executive KPIs
WITH order_revenue AS (
    SELECT
        o.order_id,
        SUM(oi.line_amount) AS net_revenue,
        SUM(oi.discount) AS total_discount
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY o.order_id
)
SELECT
    COUNT(order_id) AS total_orders,
    ROUND(SUM(net_revenue + total_discount), 2) AS gross_revenue,
    ROUND(SUM(total_discount), 2) AS total_discount,
    ROUND(SUM(net_revenue), 2) AS net_revenue,
    ROUND(SUM(net_revenue) / COUNT(order_id), 2) AS avg_order_value
FROM order_revenue;

#Revenue by Store 
WITH order_revenue AS (
    SELECT
        o.order_id,
        o.store_id,
        SUM(oi.line_amount) AS net_revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY o.order_id, o.store_id
)
SELECT
    s.store_name,
    COUNT(DISTINCT orv.order_id) AS total_orders,
    ROUND(SUM(orv.net_revenue), 2) AS store_revenue,
    ROUND(AVG(orv.net_revenue), 2) AS avg_order_value
FROM order_revenue orv
JOIN stores s
    ON orv.store_id = s.store_id
GROUP BY s.store_name
ORDER BY store_revenue DESC;

#Revenue by Sales Channel
WITH order_revenue AS (
    SELECT
        o.order_id,
        o.sales_channel,
        SUM(oi.line_amount) AS net_revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY o.order_id, o.sales_channel
)
SELECT
    sales_channel,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(net_revenue), 2) AS revenue,
    ROUND(AVG(net_revenue), 2) AS avg_order_value
FROM order_revenue
GROUP BY sales_channel;

#Monthly Revenue Trend 
WITH monthly_revenue AS (
    SELECT
        DATE_FORMAT(o.order_datetime, '%Y-%m') AS order_month,
        SUM(oi.line_amount) AS net_revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY DATE_FORMAT(o.order_datetime, '%Y-%m')
)
SELECT
    order_month,
    net_revenue,
    ROUND(
        net_revenue - LAG(net_revenue) OVER (ORDER BY order_month),
        2
    ) AS mom_change
FROM monthly_revenue
ORDER BY order_month;

#Step 2 — SALES FUNNEL & REVENUE LEAKAGE ANALYSIS

#Order Funnel Counts 
SELECT
    status,
    COUNT(DISTINCT order_id) AS total_orders
FROM orders
GROUP BY status
ORDER BY total_orders DESC;

#Funnel Conversion Percentages 
WITH funnel AS (
    SELECT
        status,
        COUNT(DISTINCT order_id) AS orders
    FROM orders
    GROUP BY status
)
SELECT
    status,
    orders,
    ROUND(
        orders * 100.0 / SUM(orders) OVER (),
        2
    ) AS pct_of_total
FROM funnel
ORDER BY orders DESC;

#Funnel by Revenue 
WITH order_revenue AS (
    SELECT
        o.order_id,
        o.status,
        SUM(oi.line_amount) AS net_revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY o.order_id, o.status
)
SELECT
    status,
    COUNT(order_id) AS orders,
    ROUND(SUM(net_revenue), 2) AS revenue
FROM order_revenue
GROUP BY status
ORDER BY revenue DESC;

#Funnel Conversion Rates 
WITH stage_counts AS (
    SELECT
        status,
        COUNT(DISTINCT order_id) AS orders
    FROM orders
    GROUP BY status
),
ordered_stages AS (
    SELECT
        status,
        orders,
        LAG(orders) OVER (
            ORDER BY FIELD(
                status,
                'created',
                'paid',
                'shipped',
                'delivered',
                'returned'
            )
        ) AS prev_stage_orders
    FROM stage_counts
)
SELECT
    status,
    orders,
    prev_stage_orders,
    ROUND(
        orders * 100.0 / prev_stage_orders,
        2
    ) AS conversion_rate_pct
FROM ordered_stages
WHERE prev_stage_orders IS NOT NULL;


#Revenue Leakage via Returns
SELECT
    ROUND(SUM(refund_amount), 2) AS total_refunds
FROM returns;

#Return Rate by Orders
WITH returned_orders AS (
    SELECT DISTINCT oi.order_id
    FROM returns r
    JOIN order_items oi
        ON r.order_item_id = oi.order_item_id
)
SELECT
    COUNT(DISTINCT ro.order_id) AS returned_orders,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(
        COUNT(DISTINCT ro.order_id) * 100.0 /
        COUNT(DISTINCT o.order_id),
        2
    ) AS return_rate_pct
FROM orders o
LEFT JOIN returned_orders ro
    ON o.order_id = ro.order_id;

#Revenue Loss % Due to Returns
WITH order_revenue AS (
    SELECT
        o.order_id,
        SUM(oi.line_amount) AS net_revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY o.order_id
),
refunds AS (
    SELECT
        oi.order_id,
        SUM(r.refund_amount) AS refunded_amount
    FROM returns r
    JOIN order_items oi
        ON r.order_item_id = oi.order_item_id
    GROUP BY oi.order_id
)
SELECT
    ROUND(SUM(refunded_amount), 2) AS total_refunded,
    ROUND(SUM(net_revenue), 2) AS total_revenue,
    ROUND(
        SUM(refunded_amount) * 100.0 / SUM(net_revenue),
        2
    ) AS revenue_loss_pct
FROM order_revenue orv
LEFT JOIN refunds rf
    ON orv.order_id = rf.order_id;

#Funnel by Sales Channel 
WITH channel_funnel AS (
    SELECT
        o.sales_channel,
        o.status,
        COUNT(DISTINCT o.order_id) AS orders
    FROM orders o
    GROUP BY o.sales_channel, o.status
)
SELECT
    sales_channel,
    status,
    orders,
    ROUND(
        orders * 100.0 /
        SUM(orders) OVER (PARTITION BY sales_channel),
        2
    ) AS pct_within_channel
FROM channel_funnel
ORDER BY sales_channel, orders DESC;

#Step 3 — CUSTOMER RETENTION, COHORTS & LIFETIME BEHAVIOR

#Customer Order Sequencing
WITH customer_orders AS (
    SELECT
        o.customer_id,
        o.order_id,
        o.order_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_datetime
        ) AS order_number
    FROM orders o
)
SELECT *
FROM customer_orders;

#First-Time vs Repeat Customers
WITH customer_orders AS (
    SELECT
        o.customer_id,
        o.order_id,
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_datetime
        ) AS order_number
    FROM orders o
)
SELECT
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN order_number = 1 THEN customer_id END) AS first_time_customers,
    COUNT(DISTINCT CASE WHEN order_number > 1 THEN customer_id END) AS repeat_customers,
    ROUND(
        COUNT(DISTINCT CASE WHEN order_number > 1 THEN customer_id END) * 100.0 /
        COUNT(DISTINCT customer_id),
        2
    ) AS repeat_customer_rate_pct
FROM customer_orders;

#Orders per Customer Distribution
WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders
    FROM orders
    GROUP BY customer_id
)
SELECT
    total_orders,
    COUNT(customer_id) AS customers
FROM customer_orders
GROUP BY total_orders
ORDER BY total_orders;

#Revenue: First Order vs Repeat Orders
WITH customer_order_rank AS (
    SELECT
        o.customer_id,
        o.order_id,
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_datetime
        ) AS order_number
    FROM orders o
),
order_revenue AS (
    SELECT
        o.order_id,
        SUM(oi.line_amount) AS net_revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY o.order_id
)
SELECT
    CASE
        WHEN cor.order_number = 1 THEN 'First Order'
        ELSE 'Repeat Order'
    END AS order_type,
    COUNT(DISTINCT cor.order_id) AS orders,
    ROUND(SUM(orv.net_revenue), 2) AS revenue,
    ROUND(AVG(orv.net_revenue), 2) AS avg_order_value
FROM customer_order_rank cor
JOIN order_revenue orv
    ON cor.order_id = orv.order_id
GROUP BY order_type;

#Customer Cohort Analysis
WITH first_orders AS (
    SELECT
        customer_id,
        DATE_FORMAT(MIN(order_datetime), '%Y-%m') AS cohort_month
    FROM orders
    GROUP BY customer_id
),
customer_activity AS (
    SELECT
        o.customer_id,
        DATE_FORMAT(o.order_datetime, '%Y-%m') AS activity_month,
        f.cohort_month
    FROM orders o
    JOIN first_orders f
        ON o.customer_id = f.customer_id
),
cohort_counts AS (
    SELECT
        cohort_month,
        activity_month,
        COUNT(DISTINCT customer_id) AS customers
    FROM customer_activity
    GROUP BY cohort_month, activity_month
)
SELECT
    cohort_month,
    activity_month,
    customers,
    ROUND(
        customers * 100.0 /
        FIRST_VALUE(customers) OVER (
            PARTITION BY cohort_month
            ORDER BY activity_month
        ),
        2
    ) AS retention_pct
FROM cohort_counts
ORDER BY cohort_month, activity_month;

#Retention by Customer Segment
WITH customer_orders AS (
    SELECT
        o.customer_id,
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_datetime
        ) AS order_number
    FROM orders o
)
SELECT
    c.segment,
    COUNT(DISTINCT c.customer_id) AS customers,
    COUNT(DISTINCT CASE WHEN co.order_number > 1 THEN c.customer_id END) AS repeat_customers,
    ROUND(
        COUNT(DISTINCT CASE WHEN co.order_number > 1 THEN c.customer_id END) * 100.0 /
        COUNT(DISTINCT c.customer_id),
        2
    ) AS repeat_rate_pct
FROM customers c
LEFT JOIN customer_orders co
    ON c.customer_id = co.customer_id
GROUP BY c.segment;

#Early Customer Lifetime Value
WITH customer_revenue AS (
    SELECT
        o.customer_id,
        SUM(oi.line_amount) AS lifetime_revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY o.customer_id
)
SELECT
    c.segment,
    COUNT(cr.customer_id) AS customers,
    ROUND(AVG(cr.lifetime_revenue), 2) AS avg_ltv
FROM customer_revenue cr
JOIN customers c
    ON cr.customer_id = c.customer_id
GROUP BY c.segment
ORDER BY avg_ltv DESC;

#Step 4 — PRODUCT, CATEGORY & STORE PERFORMANCE

#Product-Level Revenue
WITH product_revenue AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        SUM(oi.quantity) AS total_units_sold,
        ROUND(SUM(oi.line_amount), 2) AS net_revenue
    FROM order_items oi
    JOIN products p
        ON oi.product_id = p.product_id
    GROUP BY
        p.product_id,
        p.product_name,
        p.category,
        p.brand
)
SELECT *
FROM product_revenue;

#Top Products by Revenue
WITH product_revenue AS (
    SELECT
        p.product_name,
        SUM(oi.line_amount) AS revenue
    FROM order_items oi
    JOIN products p
        ON oi.product_id = p.product_id
    GROUP BY p.product_name
)
SELECT
    product_name,
    revenue,
    RANK() OVER (ORDER BY revenue DESC) AS revenue_rank
FROM product_revenue;

#Top Products per Category
WITH product_revenue AS (
    SELECT
        p.category,
        p.product_name,
        SUM(oi.line_amount) AS revenue
    FROM order_items oi
    JOIN products p
        ON oi.product_id = p.product_id
    GROUP BY p.category, p.product_name
)
SELECT *
FROM (
    SELECT
        category,
        product_name,
        revenue,
        DENSE_RANK() OVER (
            PARTITION BY category
            ORDER BY revenue DESC
        ) AS category_rank
    FROM product_revenue
) ranked
WHERE category_rank <= 3;

#Category Contribution to Total Revenue (% Share)
WITH category_revenue AS (
    SELECT
        p.category,
        SUM(oi.line_amount) AS revenue
    FROM order_items oi
    JOIN products p
        ON oi.product_id = p.product_id
    GROUP BY p.category
)
SELECT
    category,
    revenue,
    ROUND(
        revenue * 100.0 / SUM(revenue) OVER (),
        2
    ) AS revenue_share_pct
FROM category_revenue
ORDER BY revenue DESC;

#Store-Level Revenue & Ranking
WITH store_revenue AS (
    SELECT
        s.store_name,
        SUM(oi.line_amount) AS revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    JOIN stores s
        ON o.store_id = s.store_id
    GROUP BY s.store_name
)
SELECT
    store_name,
    revenue,
    RANK() OVER (ORDER BY revenue DESC) AS store_rank
FROM store_revenue;

#Store Revenue Contribution (%)
WITH store_revenue AS (
    SELECT
        s.store_name,
        SUM(oi.line_amount) AS revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    JOIN stores s
        ON o.store_id = s.store_id
    GROUP BY s.store_name
)
SELECT
    store_name,
    revenue,
    ROUND(
        revenue * 100.0 / SUM(revenue) OVER (),
        2
    ) AS revenue_contribution_pct
FROM store_revenue
ORDER BY revenue DESC;

#Running Revenue by Store (Trend Insight)
WITH store_monthly_revenue AS (
    SELECT
        s.store_name,
        DATE_FORMAT(o.order_datetime, '%Y-%m') AS order_month,
        SUM(oi.line_amount) AS revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    JOIN stores s
        ON o.store_id = s.store_id
    GROUP BY s.store_name, DATE_FORMAT(o.order_datetime, '%Y-%m')
)
SELECT
    store_name,
    order_month,
    revenue,
    SUM(revenue) OVER (
        PARTITION BY store_name
        ORDER BY order_month
    ) AS running_revenue
FROM store_monthly_revenue
ORDER BY store_name, order_month;

#Profitability Proxy
WITH product_profit AS (
    SELECT
        p.product_name,
        SUM(oi.quantity * p.unit_cost) AS total_cost,
        SUM(oi.line_amount) AS revenue
    FROM order_items oi
    JOIN products p
        ON oi.product_id = p.product_id
    GROUP BY p.product_name
)
SELECT
    product_name,
    ROUND(revenue - total_cost, 2) AS profit,
    ROUND(
        (revenue - total_cost) * 100.0 / revenue,
        2
    ) AS profit_margin_pct
FROM product_profit
ORDER BY profit DESC;

#Step 5 — RETURNS, REFUNDS & PROFITABILITY IMPACT

#Return Volume & Refund Exposure
SELECT
    COUNT(DISTINCT return_id) AS total_returns,
    ROUND(SUM(refund_amount), 2) AS total_refund_amount
FROM returns;

#Return Rate by Orders
WITH returned_orders AS (
    SELECT DISTINCT oi.order_id
    FROM returns r
    JOIN order_items oi
        ON r.order_item_id = oi.order_item_id
)
SELECT
    COUNT(DISTINCT ro.order_id) AS returned_orders,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(
        COUNT(DISTINCT ro.order_id) * 100.0 /
        COUNT(DISTINCT o.order_id),
        2
    ) AS return_rate_pct
FROM orders o
LEFT JOIN returned_orders ro
    ON o.order_id = ro.order_id;

#Returns by Reason
SELECT
    reason,
    COUNT(return_id) AS total_returns,
    ROUND(SUM(refund_amount), 2) AS refund_amount
FROM returns
GROUP BY reason
ORDER BY total_returns DESC;

#Product-Level Return Risk
WITH product_returns AS (
    SELECT
        p.product_name,
        COUNT(r.return_id) AS return_count,
        ROUND(SUM(r.refund_amount), 2) AS refund_amount
    FROM returns r
    JOIN order_items oi
        ON r.order_item_id = oi.order_item_id
    JOIN products p
        ON oi.product_id = p.product_id
    GROUP BY p.product_name
),
product_sales AS (
    SELECT
        p.product_name,
        SUM(oi.quantity) AS units_sold
    FROM order_items oi
    JOIN products p
        ON oi.product_id = p.product_id
    GROUP BY p.product_name
)
SELECT
    ps.product_name,
    ps.units_sold,
    COALESCE(pr.return_count, 0) AS returns,
    ROUND(
        COALESCE(pr.return_count, 0) * 100.0 / ps.units_sold,
        2
    ) AS return_rate_pct,
    COALESCE(pr.refund_amount, 0) AS refund_amount
FROM product_sales ps
LEFT JOIN product_returns pr
    ON ps.product_name = pr.product_name
ORDER BY return_rate_pct DESC;

#Revenue vs Refund Impact
WITH revenue AS (
    SELECT
        SUM(oi.line_amount) AS total_revenue
    FROM order_items oi
),
refunds AS (
    SELECT
        SUM(refund_amount) AS total_refunds
    FROM returns
)
SELECT
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(total_refunds, 2) AS total_refunds,
    ROUND(
        total_refunds * 100.0 / total_revenue,
        2
    ) AS revenue_lost_pct
FROM revenue, refunds;

#Store-Level Return Performance
WITH store_returns AS (
    SELECT
        s.store_name,
        COUNT(r.return_id) AS returns,
        ROUND(SUM(r.refund_amount), 2) AS refund_amount
    FROM returns r
    JOIN order_items oi
        ON r.order_item_id = oi.order_item_id
    JOIN orders o
        ON oi.order_id = o.order_id
    JOIN stores s
        ON o.store_id = s.store_id
    GROUP BY s.store_name
),
store_orders AS (
    SELECT
        s.store_name,
        COUNT(DISTINCT o.order_id) AS orders
    FROM orders o
    JOIN stores s
        ON o.store_id = s.store_id
    GROUP BY s.store_name
)
SELECT
    so.store_name,
    so.orders,
    COALESCE(sr.returns, 0) AS returns,
    ROUND(
        COALESCE(sr.returns, 0) * 100.0 / so.orders,
        2
    ) AS return_rate_pct,
    COALESCE(sr.refund_amount, 0) AS refund_amount
FROM store_orders so
LEFT JOIN store_returns sr
    ON so.store_name = sr.store_name
ORDER BY return_rate_pct DESC;

#Profit Impact After Returns
WITH revenue_cost AS (
    SELECT
        p.product_name,
        SUM(oi.line_amount) AS revenue,
        SUM(oi.quantity * p.unit_cost) AS cost
    FROM order_items oi
    JOIN products p
        ON oi.product_id = p.product_id
    GROUP BY p.product_name
),
refunds AS (
    SELECT
        p.product_name,
        SUM(r.refund_amount) AS refunds
    FROM returns r
    JOIN order_items oi
        ON r.order_item_id = oi.order_item_id
    JOIN products p
        ON oi.product_id = p.product_id
    GROUP BY p.product_name
)
SELECT
    rc.product_name,
    ROUND(rc.revenue - rc.cost - COALESCE(rf.refunds, 0), 2) AS net_profit_after_returns
FROM revenue_cost rc
LEFT JOIN refunds rf
    ON rc.product_name = rf.product_name
ORDER BY net_profit_after_returns DESC;

#Step 6 — INVENTORY MOVEMENT & OPERATIONAL ANALYTICS

#Inventory Movement Classification
SELECT
    movement_id,
    store_id,
    product_id,
    movement_datetime,
    movement_type,
    quantity_delta
FROM inventory_movements;

#Net Inventory Change by Store & Product
WITH inventory_summary AS (
    SELECT
        store_id,
        product_id,
        SUM(
            CASE
                WHEN movement_type IN ('inbound', 'return')
                    THEN quantity_delta
                ELSE -quantity_delta
            END
        ) AS net_quantity_change
    FROM inventory_movements
    GROUP BY store_id, product_id
)
SELECT *
FROM inventory_summary;

#Store-Level Inventory Pressure
WITH store_inventory AS (
    SELECT
        store_id,
        SUM(
            CASE
                WHEN movement_type IN ('inbound', 'return')
                    THEN quantity_delta
                ELSE -quantity_delta
            END
        ) AS net_inventory_change
    FROM inventory_movements
    GROUP BY store_id
)
SELECT
    s.store_name,
    net_inventory_change
FROM store_inventory si
JOIN stores s
    ON si.store_id = s.store_id
ORDER BY net_inventory_change DESC;

#Returns Feeding Back Into Inventory
WITH return_inventory AS (
    SELECT
        store_id,
        SUM(quantity_delta) AS return_units
    FROM inventory_movements
    WHERE movement_type = 'return'
    GROUP BY store_id
)
SELECT
    s.store_name,
    COALESCE(ri.return_units, 0) AS return_units
FROM stores s
LEFT JOIN return_inventory ri
    ON s.store_id = ri.store_id
ORDER BY return_units DESC;

#Inventory vs Sales Mismatch
WITH sales_units AS (
    SELECT
        o.store_id,
        oi.product_id,
        SUM(oi.quantity) AS units_sold
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY o.store_id, oi.product_id
),
inventory_units AS (
    SELECT
        store_id,
        product_id,
        SUM(
            CASE
                WHEN movement_type IN ('inbound', 'return')
                    THEN quantity_delta
                ELSE -quantity_delta
            END
        ) AS net_units
    FROM inventory_movements
    GROUP BY store_id, product_id
)
SELECT
    s.store_name,
    su.product_id,
    su.units_sold,
    COALESCE(iu.net_units, 0) AS net_inventory_change
FROM sales_units su
JOIN stores s
    ON su.store_id = s.store_id
LEFT JOIN inventory_units iu
    ON su.store_id = iu.store_id
   AND su.product_id = iu.product_id
ORDER BY units_sold DESC;

#Time-Based Inventory Flow
WITH inventory_timeline AS (
    SELECT
        store_id,
        product_id,
        DATE(movement_datetime) AS movement_date,
        SUM(
            CASE
                WHEN movement_type IN ('inbound', 'return')
                    THEN quantity_delta
                ELSE -quantity_delta
            END
        ) AS daily_change
    FROM inventory_movements
    GROUP BY store_id, product_id, DATE(movement_datetime)
)
SELECT
    store_id,
    product_id,
    movement_date,
    daily_change,
    SUM(daily_change) OVER (
        PARTITION BY store_id, product_id
        ORDER BY movement_date
    ) AS running_inventory_change
FROM inventory_timeline
ORDER BY store_id, product_id, movement_date;
