import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)


def add_task(task_type, data):
    task_id = f"{task_type}_{int(time.time()*1000)}"
    task = json.dumps({"id": task_id, "type": task_type, "data": data})
    r.lpush("queue:tasks", task)
    print(f"Added {task_id}")


def main():
    add_task("email", {"to": "user@example.com", "subject": "Test"})
    add_task("order", {"order_id": 456, "user_id": 789})
    add_task("log", {"message": "System event", "level": "info"})


if __name__ == "__main__":
    main()
