import json
import time
import threading

import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

_shutdown = threading.Event()


def process_task():
    thread_id = threading.get_ident()
    while not _shutdown.is_set():
        try:
            result = r.brpop("queue:tasks", timeout=1)
            if result and result[1]:
                task = json.loads(result[1])
                task_id = task["id"]
                if task["type"] == "email":
                    time.sleep(2)
                    print(f"[Thread {thread_id}] Sent email for {task_id}: {task['data']}")
                elif task["type"] == "order":
                    time.sleep(3)
                    print(f"[Thread {thread_id}] Processed order {task['data']['order_id']} for {task_id}")
                elif task["type"] == "log":
                    print(f"[Thread {thread_id}] Logged: {task['data']['message']} ({task_id})")
        except KeyboardInterrupt:
            _shutdown.set()
        except Exception as e:
            print(f"[Thread {thread_id}] Error: {e}")


def main():
    threads = [threading.Thread(target=process_task) for _ in range(2)]
    for thread in threads:
        thread.start()

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nShutting down consumers...")
        _shutdown.set()

    for thread in threads:
        thread.join(timeout=2)


if __name__ == "__main__":
    main()
