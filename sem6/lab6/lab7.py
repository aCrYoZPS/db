from itertools import combinations
from collections import Counter
from datetime import datetime
import random
import time

from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure
from pymongo.synchronous.database import Database

client = MongoClient("localhost", 27016, replicaSet="rs0", directConnection=True)
db: Database = client["sem6"]


def monthly_revenue_by_category() -> dict:
    pipeline = [
        {
            "$lookup": {
                "from": "orders",
                "localField": "order_id",
                "foreignField": "_id",
                "as": "order",
            }
        },
        {"$unwind": "$order"},
        {"$match": {"order.status": {"$ne": "cancelled"}}},
        {
            "$lookup": {
                "from": "products",
                "localField": "product_id",
                "foreignField": "_id",
                "as": "product",
            }
        },
        {"$unwind": "$product"},
        {
            "$lookup": {
                "from": "categories",
                "localField": "product.category_id",
                "foreignField": "_id",
                "as": "category",
            }
        },
        {"$unwind": "$category"},
        {
            "$group": {
                "_id": {
                    "month": {"$dateToString": {"format": "%Y-%m",
                                                "date": "$order.created_at"}},
                    "category": "$category.name",
                },
                "revenue": {"$sum": {"$multiply": ["$quantity", "$price"]}},
                "orders_count": {"$addToSet": "$order_id"},
                "items_sold": {"$sum": "$quantity"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "month": "$_id.month",
                "category": "$_id.category",
                "revenue": {"$round": ["$revenue", 2]},
                "orders_count": {"$size": "$orders_count"},
                "items_sold": 1,
            }
        },
        {"$sort": {"month": 1, "revenue": -1}},
    ]
    rows = list(db["order_items"].aggregate(pipeline))

    result: dict = {}
    for row in rows:
        cat = row["category"]
        month = row["month"]
        result.setdefault(cat, {})[month] = {
            "revenue": row["revenue"],
            "orders_count": row["orders_count"],
            "items_sold": row["items_sold"],
        }

    return result


def market_basket_analysis(min_support: int = 2, limit: int = 10) -> list[dict]:
    pipeline = [
        {
            "$group": {
                "_id": "$order_id",
                "products": {"$addToSet": "$product_id"},
            }
        },
        {"$match": {"products.1": {"$exists": True}}},
    ]

    basket_counter = Counter()
    for order in db["order_items"].aggregate(pipeline):
        prods = sorted(order["products"])
        for pair in combinations(prods, 2):
            basket_counter[pair] += 1

    top_pairs = [
                    {"product_a": a, "product_b": b, "co_occurrences": cnt}
                    for (a, b), cnt in basket_counter.most_common(limit * 3)
                    if cnt >= min_support
                ][:limit]

    all_ids = list({p for pair in top_pairs
                    for p in [pair["product_a"], pair["product_b"]]})
    products_map = {
        p["_id"]: p["name"]
        for p in db["products"].find({"_id": {"$in": all_ids}}, {"name": 1})
    }
    for pair in top_pairs:
        pair["name_a"] = products_map.get(pair["product_a"], "N/A")
        pair["name_b"] = products_map.get(pair["product_b"], "N/A")

    return top_pairs


def rfm_analysis(top_n: int = 10) -> list[dict]:
    now = datetime.now()

    pipeline = [
        {"$match": {"status": {"$in": ["completed", "shipped"]}}},
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
                "order_total": {
                    "$sum": {
                        "$map": {
                            "input": "$items",
                            "as": "i",
                            "in": {"$multiply": ["$$i.quantity", "$$i.price"]},
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "last_order": {"$max": "$created_at"},
                "frequency": {"$sum": 1},
                "monetary": {"$sum": "$order_total"},
            }
        },
        {
            "$addFields": {
                "recency_days": {
                    "$divide": [
                        {"$subtract": [now, "$last_order"]},
                        1000 * 60 * 60 * 24,
                    ]
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "user_id": "$_id",
                "recency_days": {"$round": ["$recency_days", 0]},
                "frequency": 1,
                "monetary": {"$round": ["$monetary", 2]},
            }
        },
        {"$sort": {"monetary": -1}},
        {"$limit": top_n},
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user_info",
            }
        },
        {
            "$addFields": {
                "name": {"$arrayElemAt": ["$user_info.name", 0]}
            }
        },
        {"$project": {"user_info": 0}},
    ]

    results = list(db["orders"].aggregate(pipeline))

    if results:
        max_m = max(r["monetary"] for r in results)
        max_f = max(r["frequency"] for r in results)
        max_r = max(r["recency_days"] for r in results)

        for r in results:
            m_score = round(r["monetary"] / max_m * 5) if max_m else 1
            f_score = round(r["frequency"] / max_f * 5) if max_f else 1
            r_score = round((1 - r["recency_days"] / max_r) * 5) if max_r else 1

            r["rfm_score"] = f"{r_score}{f_score}{m_score}"
            r["segment"] = (
                "Champions" if r_score >= 4 and f_score >= 4 and m_score >= 4 else
                "Loyal" if f_score >= 4 else
                "Big Spenders" if m_score >= 4 else
                "At Risk" if r_score <= 2 else
                "Potential"
            )

    return results


def run_task1():
    print("=== TASK 1: Business Metrics ===")

    print("\n Monthly Revenue by Category ---")
    data = monthly_revenue_by_category()
    for category, months in sorted(data.items()):
        print(f"\n  {category}")
        for month, metrics in sorted(months.items()):
            print(f"    {month}  revenue=${metrics['revenue']:>9.2f}  "
                  f"orders={metrics['orders_count']}  "
                  f"items={metrics['items_sold']}")

    print("\n Market Basket Analysis ---")
    for p in market_basket_analysis(min_support=1, limit=5):
        print(f"  [{p['co_occurrences']}x]  {p['name_a'][:28]} + {p['name_b'][:28]}")

    print("\n--- RFM Analysis (top 10) ---")
    print(f"  {'Name':<20} {'Recency':>8} {'Freq':>5} {'Monetary':>10}  Score  Segment")
    print("  " + "─" * 68)
    for r in rfm_analysis(top_n=10):
        print(f"  {str(r.get('name', '?')):<20} "
              f"{int(r['recency_days']):>7}d "
              f"{r['frequency']:>5} "
              f"${r['monetary']:>9.2f}  "
              f"  {r['rfm_score']}  {r['segment']}")
    print()


CACHE_COLLECTION = "mv_monthly_revenue"


def build_materialized_view_out():
    pipeline = [
        {"$lookup": {"from": "orders", "localField": "order_id",
                     "foreignField": "_id", "as": "order"}},
        {"$unwind": "$order"},
        {"$lookup": {"from": "products", "localField": "product_id",
                     "foreignField": "_id", "as": "product"}},
        {"$unwind": "$product"},
        {"$lookup": {"from": "categories", "localField": "product.category_id",
                     "foreignField": "_id", "as": "category"}},
        {"$unwind": "$category"},
        {
            "$group": {
                "_id": {
                    "month": {"$dateToString": {"format": "%Y-%m",
                                                "date": "$order.created_at"}},
                    "category": "$category.name",
                },
                "revenue": {"$sum": {"$multiply": ["$quantity", "$price"]}},
                "items_sold": {"$sum": "$quantity"},
                "orders_count": {"$addToSet": "$order_id"},
            }
        },
        {
            "$project": {
                "month": "$_id.month",
                "category": "$_id.category",
                "revenue": {"$round": ["$revenue", 2]},
                "items_sold": 1,
                "orders_count": {"$size": "$orders_count"},
                "refreshed_at": {"$literal": datetime.now()},
            }
        },
        {"$out": CACHE_COLLECTION},
    ]
    db["order_items"].aggregate(pipeline)
    db[CACHE_COLLECTION].create_index([("month", ASCENDING), ("category", ASCENDING)])
    print(f"  $out view '{CACHE_COLLECTION}': "
          f"{db[CACHE_COLLECTION].count_documents({})} docs")


def compare_live_vs_materialized(runs: int = 20):
    print("\n=== TASK 2: Materialized View Performance ===")
    build_materialized_view_out()

    live_times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        monthly_revenue_by_category()
        live_times.append((time.perf_counter() - t0) * 1000)

    cached_times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        list(db[CACHE_COLLECTION].find({}, {"_id": 0})
             .sort([("month", 1), ("revenue", -1)]))
        cached_times.append((time.perf_counter() - t0) * 1000)

    avg_live = sum(live_times) / runs
    avg_cached = sum(cached_times) / runs
    speedup = avg_live / avg_cached if avg_cached > 0 else float("inf")

    print(f"  Live aggregation  : {avg_live:.2f} ms  (avg {runs} runs)")
    print(f"  Materialized view : {avg_cached:.2f} ms  (avg {runs} runs)")
    print(f"  Speedup         : {speedup:.1f}x")

    doc = db[CACHE_COLLECTION].find_one({}, {"refreshed_at": 1})
    if doc and "refreshed_at" in doc:
        age = (datetime.now() - doc["refreshed_at"]).seconds
        print(f"  View freshness  : refreshed {age}s ago")
    print()


def setup_warehouses():
    db["warehouses"].drop()
    db["inventory_history"].drop()

    db["warehouses"].insert_many([
        {"_id": "WH_NORTH", "name": "North Warehouse",
         "location": "Minsk", "inventory": {}},
        {"_id": "WH_SOUTH", "name": "South Warehouse",
         "location": "Brest", "inventory": {}},
        {"_id": "WH_EAST", "name": "East Warehouse",
         "location": "Vitebsk", "inventory": {}},
    ])

    products = list(db["products"].find({}, {"_id": 1}).limit(5))
    for wh_id in ["WH_NORTH", "WH_SOUTH", "WH_EAST"]:
        inventory = {str(p["_id"]): random.randint(10, 100) for p in products}
        db["warehouses"].update_one({"_id": wh_id}, {"$set": {"inventory": inventory}})

    print("  Warehouses initialized:")
    for wh in db["warehouses"].find():
        total = sum(wh["inventory"].values())
        print(f"    {wh['_id']}: {total} units across {len(wh['inventory'])} SKUs")

    return [str(p["_id"]) for p in products]


def transfer_inventory(from_wh: str, to_wh: str,
                       product_id: str, quantity: int) -> str:
    transfer_id = f"TRF-{int(time.time())}-{random.randint(1000, 9999)}"

    with client.start_session() as session:
        with session.start_transaction():
            result = db["warehouses"].find_one_and_update(
                {
                    "_id": from_wh,
                    f"inventory.{product_id}": {"$gte": quantity},
                },
                {"$inc": {f"inventory.{product_id}": -quantity}},
                return_document=True,
                session=session,
            )

            if result is None:
                session.abort_transaction()
                raise ValueError(
                    f"Insufficient stock: {product_id} in {from_wh} < {quantity}"
                )

            db["warehouses"].update_one(
                {"_id": to_wh},
                {"$inc": {f"inventory.{product_id}": quantity}},
                upsert=True,
                session=session,
            )

            db["inventory_history"].insert_one(
                {
                    "transfer_id": transfer_id,
                    "from_wh": from_wh,
                    "to_wh": to_wh,
                    "product_id": product_id,
                    "quantity": quantity,
                    "timestamp": datetime.now(),
                    "status": "completed",
                },
                session=session,
            )

    return transfer_id


def transfer_inventory_no_tx(from_wh: str, to_wh: str,
                             product_id: str, quantity: int) -> str:
    transfer_id = f"TRF-NONTX-{int(time.time())}"

    result = db["warehouses"].find_one_and_update(
        {"_id": from_wh, f"inventory.{product_id}": {"$gte": quantity}},
        {"$inc": {f"inventory.{product_id}": -quantity}},
        return_document=True,
    )
    if result is None:
        raise ValueError(f"Insufficient stock: {product_id} in {from_wh}")

    db["warehouses"].update_one(
        {"_id": to_wh},
        {"$inc": {f"inventory.{product_id}": quantity}},
        upsert=True,
    )
    db["inventory_history"].insert_one({
        "transfer_id": transfer_id,
        "from_wh": from_wh,
        "to_wh": to_wh,
        "product_id": product_id,
        "quantity": quantity,
        "timestamp": datetime.now(),
        "status": "completed_no_tx",
    })
    return transfer_id


def demo_warehouse_transfer():
    print("\n=== TASK 3: Warehouse Inventory Transfer ===\n")
    product_ids = setup_warehouses()
    pid = product_ids[0]

    wh_n = db["warehouses"].find_one({"_id": "WH_NORTH"})
    wh_s = db["warehouses"].find_one({"_id": "WH_SOUTH"})
    qty = min(wh_n["inventory"].get(pid, 0) - 1, 5)

    print(f"\n  Transferring {qty} units of product '{pid}': WH_NORTH → WH_SOUTH")
    print(f"  Before — WH_NORTH: {wh_n['inventory'].get(pid, 0)}  "
          f"WH_SOUTH: {wh_s['inventory'].get(pid, 0)}")

    try:
        tid = transfer_inventory("WH_NORTH", "WH_SOUTH", pid, qty)
    except OperationFailure:
        print("  (Replica set not available, using non-transactional fallback)")
        tid = transfer_inventory_no_tx("WH_NORTH", "WH_SOUTH", pid, qty)

    wh_n2 = db["warehouses"].find_one({"_id": "WH_NORTH"})
    wh_s2 = db["warehouses"].find_one({"_id": "WH_SOUTH"})
    print(f"  Transfer ID: {tid}")
    print(f"  After  — WH_NORTH: {wh_n2['inventory'].get(pid, 0)}  "
          f"WH_SOUTH: {wh_s2['inventory'].get(pid, 0)}")

    print("\n  Testing insufficient stock (requesting 9999 units)...")
    try:
        transfer_inventory_no_tx("WH_NORTH", "WH_SOUTH", pid, 9999)
    except ValueError as e:
        print(f"  Correctly rejected: {e}")

    history = list(db["inventory_history"].find(
        {"product_id": pid}, {"_id": 0, "transfer_id": 1, "quantity": 1, "status": 1}
    ))
    print(f"\n  Audit log ({len(history)} records):")
    for h in history:
        print(f"    {h['transfer_id']}  qty={h['quantity']}  [{h['status']}]")
    print()


def setup_last_item(product_id: str, quantity: int = 1):
    db["stock"].drop()
    db["stock"].insert_one({
        "_id": product_id,
        "quantity": quantity,
        "updated_at": datetime.now(),
    })
    print(f"  Stock: product={product_id}, quantity={quantity}")


def buy_no_tx(product_id: str, results: list, idx: int):
    time.sleep(random.uniform(0, 0.01))
    doc = db["stock"].find_one({"_id": product_id})

    if doc and doc["quantity"] > 0:
        time.sleep(random.uniform(0, 0.005))
        db["stock"].update_one(
            {"_id": product_id},
            {"$inc": {"quantity": -1}, "$set": {"updated_at": datetime.now()}}
        )
        results[idx] = {"user": idx + 1, "status": "purchased",
                        "saw_qty": doc["quantity"]}
    else:
        results[idx] = {"user": idx + 1, "status": "out_of_stock",
                        "saw_qty": doc["quantity"] if doc else 0}


def buy_with_tx(product_id: str, user_id: int, results: list, idx: int):
    for attempt in range(1, 4):
        try:
            with client.start_session() as session:
                with session.start_transaction():
                    result = db["stock"].find_one_and_update(
                        {"_id": product_id, "quantity": {"$gt": 0}},
                        {"$inc": {"quantity": -1},
                         "$set": {"updated_at": datetime.now()}},
                        return_document=True,
                        session=session,
                    )
                    if result is None:
                        session.abort_transaction()
                        results[idx] = {"user": user_id, "status": "out_of_stock",
                                        "attempts": attempt}
                        return
                    results[idx] = {"user": user_id, "status": "purchased",
                                    "qty_left": result["quantity"], "attempts": attempt}
                    return

        except OperationFailure as e:
            if e.has_error_label("TransientTransactionError"):
                print("Write conflict, retry needed.")
                if attempt < 3:
                    time.sleep(0.01 * (2 ** attempt))
                else:
                    results[idx] = {"user": user_id, "status": "write_conflict_max_retries"}

            result = db["stock"].find_one_and_update(
                {"_id": product_id, "quantity": {"$gt": 0}},
                {"$inc": {"quantity": -1},
                 "$set": {"updated_at": datetime.utcnow()}},
                return_document=True,
            )
            if result is None:
                results[idx] = {"user": user_id, "status": "out_of_stock (atomic)",
                                "attempts": attempt}
            else:
                results[idx] = {"user": user_id, "status": "purchased (atomic)",
                                "qty_left": result["quantity"], "attempts": attempt}


def demo_isolation():
    print("\n=== TASK 4: Isolation & WriteConflict ===")

    pid = str(db["products"].find_one({}, {"_id": 1})["_id"])

    # ── 4a: Гонка БЕЗ транзакций ─────────────────────────────────────────────
    print("\n--- 4a) WITHOUT transactions (race condition) ---")
    setup_last_item(pid, quantity=1)

    results_no_tx = [None, None]
    threads = [threading.Thread(target=buy_no_tx, args=(pid, results_no_tx, i))
               for i in range(2)]
    for t in threads: t.start()
    for t in threads: t.join()

    final = db["stock"].find_one({"_id": pid})
    print(f"  User 1: {results_no_tx[0]}")
    print(f"  User 2: {results_no_tx[1]}")
    print(f"  Final quantity: {final['quantity']}")

    if all(r["status"] == "purchased" for r in results_no_tx):
        if final["quantity"] < 0:
            print("  ⚠  RACE CONDITION: stock went negative — OVERSOLD!")
        else:
            print("  ⚠  Both purchased (lucky ordering this run — race still possible)")
    else:
        print("  (One thread lost the race — but not guaranteed without transactions)")

    # ── 4b: С транзакцией — WriteConflict защищает ───────────────────────────
    print("\n--- 4b) WITH transactions (WriteConflict protection) ---")
    setup_last_item(pid, quantity=1)

    results_tx = [None, None]
    threads = [threading.Thread(target=buy_with_tx, args=(pid, i + 1, results_tx, i))
               for i in range(2)]
    for t in threads: t.start()
    for t in threads: t.join()

    final_tx = db["stock"].find_one({"_id": pid})
    print(f"  User 1: {results_tx[0]}")
    print(f"  User 2: {results_tx[1]}")
    print(f"  Final quantity: {final_tx['quantity']}")

    purchased = sum(1 for r in results_tx if r and "purchased" in r.get("status", ""))
    if purchased == 1 and final_tx["quantity"] == 0:
        print("  ✔ CORRECT: exactly 1 purchase, stock = 0, no oversell")
    print()


def main():
    # run_task1()
    # compare_live_vs_materialized()
    demo_warehouse_transfer()


if __name__ == '__main__':
    main()
