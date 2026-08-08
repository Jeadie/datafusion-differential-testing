SELECT o.id, c.code, o.amount
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
ORDER BY o.id;
