import time
import redis


r = redis.Redis(host='localhost', port=6379, decode_responses=True)

with open("sliding_window.lua", "r") as f:
    lua_script = f.read()

script_sha = r.script_load(lua_script)


def sliding_rate_limit(user_id, max_req=10, window_ms=600000):
    key = f"rate_sliding:{user_id}"
    now = int(time.time() * 1000)
    req_id = f"{now}:{int(time.time_ns() % 1000000)}"

    result = r.evalsha(script_sha, 1, key, window_ms, max_req, now, req_id)
    allowed = result[0] == 1
    return allowed, result[1], result[2]


def main():
    for _ in range(11):
        allowed, current, retry = sliding_rate_limit("user123")
        if allowed:
            print(f"Current: {current}")
        else:
            print(f"Retry at: {retry}")


if __name__ == "__main__":
    main()
