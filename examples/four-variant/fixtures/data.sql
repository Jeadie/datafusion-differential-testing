CREATE TABLE orders (
    id BIGINT,
    customer_id BIGINT,
    amount BIGINT
);
INSERT INTO orders VALUES
    (1, 10, 10),
    (2, 20, 20),
    (3, 10, 20),
    (4, NULL, 5);

CREATE TABLE customers (
    id BIGINT,
    code BIGINT
);
INSERT INTO customers VALUES
    (10, 100),
    (20, 200);

