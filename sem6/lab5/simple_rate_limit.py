import time
import redis


r = redis.Redis(host='localhost', port=6379, decode_responses=True)


def check_rate_limit(user_id, limit=100, window=600):
    now = time.time()
    window_start = int(now // window) * window
    key = f"rate:{user_id}:{window_start}"

    count = r.incr(key)
    if count == 1:
        r.expire(key, window)

    if count > limit:
        return False, count - limit
    return True, limit - count


user_id = "user123"
allowed, remaining = check_rate_limit(user_id)
if allowed:
    print(f"Allowed, remaining: {remaining}")
else:
    print("Rate limited")
