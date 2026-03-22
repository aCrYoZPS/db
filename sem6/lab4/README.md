# Основные use cases и SQL‑операции
## Последние заказы пользователя


```sql
SELECT o.*, oi.*
FROM orders o
JOIN order_items oi ON oi.oreder_id = o.order_id
WHERE o.user_id = :user_id
ORDER BY o.created_at DESC
LIMIT 10;
```
Используются: ORDER BY, LIMIT, JOIN.

Для вывода с позициями: JOIN order_items oi ON oi.order_id = o.order_id и, при необходимости, JOIN products p ....

## Топ‑N популярных товаров (по количеству/выручке)

```sql
SELECT oi.product_id,
       SUM(oi.quantity)          AS total_qty,
       SUM(oi.quantity*oi.price) AS revenue
FROM order_items oi
GROUP BY oi.product_id
ORDER BY total_qty DESC
LIMIT :n;
```
Используются: GROUP BY, ORDER BY, LIMIT (+ JOIN products p для имени товара).

## Все товары категории с фильтрацией по цене

```sql
SELECT p.*, c.name
FROM products p
JOIN categories c on c.id = p.category_id
WHERE p.category_id = :cat_id
AND p.price BETWEEN :min_price AND :max_price
ORDER BY p.price;
```

## Выручка по пользователю

```sql
SELECT o.user_id,
       SUM(oi.quantity*oi.price) AS revenue
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY o.user_id
HAVING o.user_id = :user_id;
```
Используются: JOIN, GROUP BY, HAVING.

## История покупок пользователя / просмотренные товары
```sql
SELECT 
    o.order_id,
    o.created_at,
    o.status,
    oi.order_item_id,
    oi.quantity,
    oi.price,
    p.product_id,
    p.name AS product_name,
    c.name AS category_name,
    (oi.quantity * oi.price) AS total_price
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
JOIN products p ON p.product_id = oi.product_id
JOIN categories c ON c.category_id = p.category_id
WHERE o.user_id = :user_id  -- например, 123
ORDER BY o.created_at DESC, oi.order_item_id;
```

## Пользователи с наибольшим пересечением товаров
```sql
WITH user_products AS (
    SELECT 
        o.user_id,
        p.product_id
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    JOIN products p ON p.product_id = oi.product_id
    GROUP BY o.user_id, p.product_id
),
target_user_products AS (
    SELECT product_id
    FROM user_products
    WHERE user_id = :user_id
),
similar_users AS (
    SELECT 
        up2.user_id AS similar_user_id,
        COUNT(up2.product_id) AS common_products,
        (SELECT COUNT(*) FROM target_user_products) AS target_products,
        (SELECT COUNT(*) FROM user_products up3 WHERE up3.user_id = up2.user_id) AS similar_total_products
    FROM user_products up2
    JOIN target_user_products tup ON tup.product_id = up2.product_id
    WHERE up2.user_id != :user_id
    GROUP BY up2.user_id
    HAVING common_products >= 2
)
SELECT 
    su.similar_user_id,
    su.common_products,
    ROUND(su.common_products * 100.0 / su.similar_total_products, 2) AS similarity_percent
FROM similar_users su
ORDER BY su.common_products DESC, similarity_percent DESC
LIMIT 10;
```

# Моделирование в Redis: типы данных и ключи
Общие соглашения по ключам
*Сущности:* user:{id}, product:{id}, category:{id}, order:{id}, order_item:{id} — Hash.

*Множества/связи:* user:{id}:orders, order:{id}:items, category:{id}:products, user:{id}:viewed, user:{id}:purchased — Set либо List (там, где важен порядок).

Индексы и агрегаты:

*Sorted Set:* products:by_sales, products:by_revenue, products:by_price.

*Кэши:* cache:top_products:by_sales:{N}, cache:top_products:by_revenue:{N}

*Доп. индексы:* index:user:email:{email} -> user_id

| SQL сущность / запрос           | Redis структура        | Ключи / замечания                                                                                                                     |
| ------------------------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Таблица users                   | Hash                   | user:{user_id}; поля: name, email, created_at.                                                                                        |
| Индекс по email (UNIQUE)        | String / Set           | index:user:email:{email} = {user_id}.                                                                                                 |
| Таблица categories              | Hash                   | category:{category_id} с name.                                                                                                        |
| Таблица products                | Hash + индексы         | product:{product_id}; Sorted Set products:by_price (score = price); Set category:{id}:products содержит product_id.                   |
| Таблица orders                  | Hash + связи           | order:{order_id}; List или Sorted Set user:{user_id}:orders (score/порядок по created_at).                                            |
| Таблица order_items             | Hash + связи           | order_item:{order_item_id}; Set/List order:{order_id}:items с order_item_id.                                                          |
| «Последние заказы пользователя» | List/Sorted Set + Hash | Храним order_id в user:{id}:orders; читаем последние 10, затем HMGET по order:{id} и order:{id}:items.                                |
| «Топ N по продажам»             | Sorted Set             | ZINCRBY products:by_sales {qty} {product_id} при каждой покупке; ZREVRANGE ... 0 N-1 WITHSCORES.                                      |
| «Топ N по выручке»              | Sorted Set             | ZINCRBY products:by_revenue {qty*price} {product_id}; аналогично.                                                                     |
| «Товары по категории и цене»    | Set + Sorted Set       | SMEMBERS category:{id}:products ∩ ZRANGEBYSCORE products:by_price +  фильтрация по цене в коде.                                       |
| «Похожие пользователи»          | Sets                   | user:{id}:viewed, user:{id}:purchased; операции SINTER, SUNION, SDIFF для анализа пересечений. codezup                                |
| «Кэш топ‑товаров»               | String                 | SET cache:top_products:by_sales:{N} {json} EX 60. image.jpg                                                                           |
