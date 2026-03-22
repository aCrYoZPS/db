import os
import csv
import pprint
from datetime import datetime
import json

import redis

DATA_DIR = "./data/"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def load_users():
    path = os.path.join(DATA_DIR, "userid-name-email-createdat.csv")
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            user_id = row["user_id"]
            key = f"user:{user_id}"
            r.hset(key, mapping={
                "name": row["name"],
                "email": row["email"],
                "created_at": row["created_at"],
            })
            r.set(f"index:user:email:{row['email']}", user_id)
            r.sadd("user:all", user_id)


def load_categories():
    path = os.path.join(DATA_DIR, "categoryid-name.csv")
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cat_id = row["category_id"]
            r.hset(f"category:{cat_id}", mapping={
                "name": row["name"],
            })


def load_products():
    path = os.path.join(DATA_DIR, "productid-name-categoryid-price.csv")
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            product_id = row["product_id"]
            price = float(row["price"])
            cat_id = row["category_id"]

            r.hset(f"product:{product_id}", mapping={
                "name": row["name"],
                "category_id": cat_id,
                "price": row["price"],
            })

            r.sadd(f"category:{cat_id}:products", product_id)
            r.zadd("products:by_price", {product_id: price})


def load_orders():
    path = os.path.join(DATA_DIR, "orderid-userid-createdat-status.csv")
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            order_id = row["order_id"]
            user_id = row["user_id"]
            created_at = row["created_at"]
            created_ts = f"{datetime.strptime(created_at, TIMESTAMP_FORMAT).timestamp()}"

            r.hset(f"order:{order_id}", mapping={
                "user_id": user_id,
                "created_at": row["created_at"],
                "status": row["status"],
            })

            r.zadd(f"user:{user_id}:orders", {order_id: created_ts})


def load_order_items():
    path = os.path.join(DATA_DIR, "orderitemid-orderid-productid-quantity-price.csv")
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            order_item_id = row["order_item_id"]
            order_id = row["order_id"]
            product_id = row["product_id"]
            qty = float(row["quantity"])
            price = float(row["price"])
            revenue = qty * price

            r.hset(f"order_item:{order_item_id}", mapping={
                "order_id": order_id,
                "product_id": product_id,
                "quantity": str(qty),
                "price": str(price),
            })
            r.sadd(f"order:{order_id}:items", order_item_id)
            r.zincrby("products:by_sales", float(qty), product_id)
            r.zincrby("products:by_revenue", float(revenue), product_id)


def migrate_from_csv():
    load_users()
    load_categories()
    load_products()
    load_orders()
    load_order_items()

    fill_purchased()


def fill_purchased():
    order_keys = r.keys("order:*")
    order_ids = []

    for k in order_keys:
        if k.count(":") == 1:
            order_ids.append(k.split(":")[1])

    order_id_to_user_id = {}
    for order_id in order_ids:
        order_id_to_user_id[order_id] = r.hget(f"order:{order_id}", "user_id")

    order_item_keys = r.keys("order_item:*")
    for k in order_item_keys:
        order_id = r.hget(k, "order_id")
        product_id = r.hget(k, "product_id")
        r.sadd(f"user:{order_id_to_user_id[order_id]}:purchased", product_id)


def get_last_orders_with_items(user_id: str, limit: int = 10) -> list[dict]:
    order_ids = r.zrevrange(f"user:{user_id}:orders", 0, limit - 1)
    orders = []
    for order_id in order_ids:
        order_data = r.hgetall(f"order:{order_id}")
        item_ids = r.smembers(f"order:{order_id}:items")
        items = []
        for item_id in item_ids:
            item = r.hgetall(f"order_item:{item_id}")
            product = r.hgetall(f"product:{item['product_id']}")
            item["product"] = product
            items.append(item)
        order_data["items"] = items
        order_data["order_id"] = order_id
        orders.append(order_data)
    return orders


def get_top_products_by_sales(n: int):
    ids = r.zrevrange("products:by_sales", 0, n - 1, withscores=True)
    result = []
    for product_id, score in ids:
        product = r.hgetall(f"product:{product_id}")
        product["total_qty"] = score
        product["product_id"] = product_id
        result.append(product)
    return result


def get_top_products_by_revenue(n: int):
    ids = r.zrevrange("products:by_revenue", 0, n - 1, withscores=True)
    result = []
    for product_id, score in ids:
        product = r.hgetall(f"product:{product_id}")
        product["revenue"] = score
        product["product_id"] = product_id
        result.append(product)
    return result


def get_products_by_category_and_price(cat_id: str, min_price: float, max_price: float):
    by_price = r.zrangebyscore("products:by_price", min_price, max_price)
    by_cat = r.smembers(f"category:{cat_id}:products")
    candidates = set(by_price) & set(by_cat)
    return [r.hgetall(f"product:{pid}") for pid in candidates]


def get_most_similar_user(target_user_id: str) -> tuple[str, float]:
    target_set = f"user:{target_user_id}:purchased"
    all_users = list(r.smembers("user:all"))

    pipe = r.pipeline()
    for uid in all_users:
        pipe.sintercard(2, [target_set, f"user:{uid}:purchased"])
        pipe.scard(target_set)
        pipe.scard(f"user:{uid}:purchased")

    raw_results = pipe.execute()

    similarities = []
    for i in range(0, len(all_users) * 3, 3):
        inter = raw_results[i]
        size1 = raw_results[i + 1]
        size2 = raw_results[i + 2]

        union_size = size1 + size2 - inter
        jaccard = inter / union_size if union_size > 0 else 0

        if jaccard == 1:
            continue

        similarities.append((all_users[i // 3], jaccard))

    return sorted(similarities, key=lambda x: x[1], reverse=True)[0][0]


def get_recommended_products(user_id: str):
    other_user_id = get_most_similar_user(user_id)
    return r.sdiff(f"user:{other_user_id}:purchased", f"user:{user_id}:purchased")


def get_top_products_by_sales_cached(n: int, ttl: int = 60) -> list[dict]:
    cache_key = f"cache:top_products:by_sales:{n}"
    cached = r.get(cache_key)
    if cached:
        return json.loads(cached)

    data = get_top_products_by_sales(n)
    r.set(cache_key, json.dumps(data), ex=ttl)
    return data


def get_top_products_by_revenue_cached(n: int, ttl: int = 60) -> list[dict]:
    cache_key = f"cache:top_products:by_revenue:{n}"
    cached = r.get(cache_key)
    if cached:
        return json.loads(cached)

    data = get_top_products_by_revenue(n)
    r.set(cache_key, json.dumps(data), ex=ttl)
    return data


def main():
    # migrate_from_csv()
    # pprint.pp(get_last_orders_with_items("1"))
    n = 3
    print(f"==== Top {n} by sales ====")
    # pprint.pp(get_top_products_by_sales(n))
    print(f"==== Top {n} by revenue ====")
    pprint.pp(get_top_products_by_revenue_cached(n, 300))
    # pprint.pp(get_top_products_by_revenue(n))
    # print("==== Filtered by category and price ====")
    # pprint.pp(get_products_by_category_and_price("1", 800, 5000))
    pprint.pp(get_recommended_products("5"))
    pass


if __name__ == "__main__":
    main()
