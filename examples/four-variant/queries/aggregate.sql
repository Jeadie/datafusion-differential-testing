SELECT amount, COUNT(*) AS orders
FROM orders
GROUP BY amount
ORDER BY amount;

