import os
import time
import csv
from datetime import datetime
import pprint

from pymongo import MongoClient
from pymongo.synchronous.database import Database

DATA_DIR = "./data"
client = MongoClient("localhost", 27016, replicaSet="rs0", directConnection=True)
db: Database = client["sem6"]


def drop_all():
    for collection in db.list_collection_names():
        try:
            db.drop_collection(collection)
        except Exception as ex:
            print(f"Failed to drop collection {collection} with exception {ex}")


def load_users():
    user_collection = db["users"]

    path = os.path.join(DATA_DIR, "userid-name-email-createdat.csv")
    with open(path, newline="", encoding="utf-8") as f:
        documents = [
            {
                "_id": row["user_id"],
                "name": row["name"],
                "email": row["email"],
                "created_at": datetime.fromisoformat(row["created_at"])
            }
            for row in csv.DictReader(f)
        ]
        user_collection.insert_many(documents)


def load_categories():
    category_collection = db["categories"]

    path = os.path.join(DATA_DIR, "categoryid-name.csv")
    with open(path, newline="", encoding="utf-8") as f:
        documents = [
            {
                "_id": row["category_id"],
                "name": row["name"]
            }
            for row in csv.DictReader(f)
        ]

        category_collection.insert_many(documents)


def load_products():
    product_collection = db["products"]
    path = os.path.join(DATA_DIR, "productid-name-categoryid-price.csv")
    with open(path, newline="", encoding="utf-8") as f:
        documents = [
            {
                "_id": row["product_id"],
                "name": row["name"],
                "category_id": row["category_id"],
                "price": float(row["price"])
            }
            for row in csv.DictReader(f)
        ]
        product_collection.insert_many(documents)


def load_orders():
    order_collection = db["orders"]
    path = os.path.join(DATA_DIR, "orderid-userid-createdat-status.csv")
    with open(path, newline="", encoding="utf-8") as f:
        documents = [
            {
                "_id": row["order_id"],
                "user_id": row["user_id"],
                "created_at": datetime.fromisoformat(row["created_at"]),
                "status": row["status"]
            }
            for row in csv.DictReader(f)
        ]

        order_collection.insert_many(documents)


def load_order_items():
    order_item_collection = db["order_items"]
    path = os.path.join(DATA_DIR, "orderitemid-orderid-productid-quantity-price.csv")
    with open(path, newline="", encoding="utf-8") as f:
        with open(path, newline="", encoding="utf-8") as f:
            documents = [
                {
                    "_id": row["order_item_id"],
                    "order_id": row["order_id"],
                    "product_id": row["product_id"],
                    "quantity": int(row["quantity"]),
                    "price": float(row["price"])
                }
                for row in csv.DictReader(f)
            ]

            order_item_collection.insert_many(documents)


def migrate(drop_old: bool = True):
    if drop_old:
        drop_all()

    load_users()
    load_categories()
    load_products()
    load_orders()
    load_order_items()


def count_documents():
    print("\n--- Document counts ---")
    for col_name in ["users", "categories", "products", "orders", "order_items"]:
        count = db[col_name].count_documents({})
        print(f"  {col_name:15s}: {count} documents")


def check_user_ids():
    print("\n--- Orders with invalid user_id ---")
    pipeline_invalid_users = [
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user_docs",
            }
        },
        {"$match": {"user_docs": {"$size": 0}}},
        {"$project": {"_id": 0, "order_id": 1, "user_id": 1}},
    ]
    invalid_orders = list(db["orders"].aggregate(pipeline_invalid_users))
    if invalid_orders:
        print(f"  Found {len(invalid_orders)} orders with invalid user_id:")
        pprint.pprint(invalid_orders)
    else:
        print("  All orders have valid user_id")


def get_orders_with_total_gt(min_total: float = 1000):
    print(f"--- Orders with total revenue > {min_total} ---")
    pipeline_high_value = [
        {
            "$lookup": {
                "from": "order_items",
                "localField": "_id",
                "foreignField": "order_id",
                "as": "items",
            }
        },
        {
            "$addFields": {
                "total": {
                    "$sum": {
                        "$map": {
                            "input": "$items",
                            "as": "item",
                            "in": {"$multiply": ["$$item.quantity", "$$item.price"]},
                        }
                    }
                }
            }
        },
        {"$match": {"total": {"$gt": 1000}}},
        {
            "$project": {
                "user_id": 1,
                "status": 1,
                "total": {"$round": ["$total", 2]},
            }
        },
        {"$sort": {"total": -1}},
    ]
    high_value_orders = list(db["orders"].aggregate(pipeline_high_value))
    print(f"  Found {len(high_value_orders)} orders with total > 1000")
    for o in high_value_orders:
        print(f"    order_id={o['_id']}, user_id={o['user_id']}, "
              f"status={o['status']}, total=${o['total']:.2f}")


def check_data_integrity():
    print("=== TASK 2: Data Integrity Checks ===")
    count_documents()
    check_user_ids()
    get_orders_with_total_gt()
    print()


def get_user_orders(user_id: int) -> list[dict]:
    """SELECT * FROM orders WHERE user_id = ?"""
    return list(db["orders"].find({"user_id": user_id}))


def get_order_with_details(order_id: int) -> dict | None:
    """
    SELECT o.*, oi.*, p.*
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p     ON oi.product_id = p.product_id
    WHERE o.order_id = ?
    """
    pipeline = [
        {"$match": {"_id": order_id}},
        {
            "$lookup": {
                "from": "order_items",
                "localField": "_id",
                "foreignField": "order_id",
                "as": "items",
            }
        },
        {
            "$lookup": {
                "from": "products",
                "localField": "items.product_id",
                "foreignField": "_id",
                "as": "products_info",
            }
        },
        {
            "$addFields": {
                "items": {
                    "$map": {
                        "input": "$items",
                        "as": "item",
                        "in": {
                            "$mergeObjects": [
                                "$$item",
                                {
                                    "product": {
                                        "$arrayElemAt": [
                                            {
                                                "$filter": {
                                                    "input": "$products_info",
                                                    "as": "p",
                                                    "cond": {
                                                        "$eq": [
                                                            "$$p._id",
                                                            "$$item.product_id",
                                                        ]
                                                    },
                                                }
                                            },
                                            0,
                                        ]
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$project": {"products_info": 0, "items._id": 0}},
    ]

    results = list(db["orders"].aggregate(pipeline))

    return results[0] if results else None


def get_top_products_by_revenue(limit: int = 5) -> list[dict]:
    """
    SELECT product_id, p.name,
           SUM(oi.quantity * oi.price) AS revenue,
           SUM(oi.quantity)            AS total_sold
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY product_id
    ORDER BY revenue DESC
    LIMIT ?
    """
    pipeline = [
        {
            "$group": {
                "_id": "$product_id",
                "revenue": {"$sum": {"$multiply": ["$quantity", "$price"]}},
                "total_sold": {"$sum": "$quantity"},
            }
        },
        {"$sort": {"revenue": -1}},
        {"$limit": limit},
        {
            "$lookup": {
                "from": "products",
                "localField": "_id",
                "foreignField": "_id",
                "as": "product_info",
            }
        },
        {
            "$project": {
                "name": {"$arrayElemAt": ["$product_info.name", 0]},
                "revenue": {"$round": ["$revenue", 2]},
                "total_sold": 1,
            }
        },
    ]
    return list(db["order_items"].aggregate(pipeline))


def run_sql_analog_functions():
    print("=== TASK 3: SQL-Analog Functions ===")

    sample_user = db["users"].find_one({}, {"user_id": 1})
    if sample_user:
        uid = sample_user["_id"]
        orders = get_user_orders(uid)
        print(f"\n--- get_user_orders(user_id={uid}) ---")
        print(f"  Found {len(orders)} orders")
        for o in orders:
            print(f"    order_id={o.get('_id')}, status={o.get('status')}")

    sample_order = db["orders"].find_one({}, {"order_id": 1})
    if sample_order:
        oid = sample_order["_id"]
        detail = get_order_with_details(oid)
        print(f"\n--- 3b) get_order_with_details(order_id={oid}) ---")
        if detail:
            print(f"  status: {detail.get('status')}, items: {len(detail.get('items', []))}")
            for item in detail.get("items", [])[:2]:
                prod = item.get("product", {})
                print(f"    {prod.get('name', 'N/A')} | qty={item.get('quantity')} | ${item.get('price')}")

    print("\n--- get_top_products_by_revenue(limit=5) ---")
    for rank, p in enumerate(get_top_products_by_revenue(5), 1):
        print(f"  #{rank} {p.get('name', 'N/A'):35s} revenue=${p['revenue']:.2f}  sold={p['total_sold']}")

    print()


def run_explain(user_id: int) -> dict:
    return db.command(
        "explain",
        {"find": "orders", "filter": {"user_id": user_id}, "sort": {"created_at": -1}},
        verbosity="executionStats",
    )


def avg_query_time(user_id: int, runs: int = 50) -> float:
    total = 0.0
    for _ in range(runs):
        t0 = time.perf_counter()
        list(db["orders"].find({"user_id": user_id}).sort("created_at", -1))
        total += (time.perf_counter() - t0) * 1000
    return total / runs


def print_stats(label: str, exec_stats: dict, avg_ms: float):
    stage = (
        exec_stats.get("executionStages", {})
        .get("inputStage", {})
        .get("stage", exec_stats.get("executionStages", {}).get("stage", "N/A"))
    )
    print(f"\n--- {label} ---")
    print(f"  stage               : {stage}")
    print(f"  executionTimeMillis : {exec_stats['executionTimeMillis']} ms")
    print(f"  totalDocsExamined   : {exec_stats['totalDocsExamined']}")
    print(f"  totalKeysExamined   : {exec_stats['totalKeysExamined']}")
    print(f"  avg wall-clock      : {avg_ms:.3f} ms  (over 50 runs)")


def run_performance_test():
    print("=== TASK 4: Performance Testing ===")

    sample = db["orders"].find_one({}, {"user_id": 1})
    if not sample:
        print("  No orders found — skipping.")
        return

    uid = sample["user_id"]
    idx_name = "user_id_1_created_at_-1"

    if idx_name in db["orders"].index_information():
        db["orders"].drop_index(idx_name)

    before_stats = run_explain(uid)["executionStats"]
    before_ms = avg_query_time(uid)
    print_stats("BEFORE index", before_stats, before_ms)

    print("\n  Creating index { user_id: 1, created_at: -1 } ...")
    db["orders"].create_index(
        [("user_id", 1), ("created_at", -1)],
        name=idx_name,
    )
    print("  Index created")

    after_stats = run_explain(uid)["executionStats"]
    after_ms = avg_query_time(uid)
    print_stats("AFTER index", after_stats, after_ms)

    speedup = before_ms / after_ms if after_ms > 0 else float("inf")
    print(f"\n  Speedup: {speedup:.1f}x  ({before_ms:.2f} ms → {after_ms:.2f} ms)\n")


def main():
    migrate()
    check_data_integrity()
    run_sql_analog_functions()
    run_performance_test()


if __name__ == "__main__":
    main()
