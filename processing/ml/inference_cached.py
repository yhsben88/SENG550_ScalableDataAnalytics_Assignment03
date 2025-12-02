"""
inference_cached.py
Reads prediction from Redis instead of running model.
Usage: python inference_cached.py <day_of_week> <hour_of_day> <category>
"""
import sys
import redis
REDIS_HOST = "localhost"
REDIS_PORT = 6379

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python inference_cached.py <day_of_week> <hour_of_day> <category>")
        sys.exit(1)
    key = f"{int(sys.argv[1])}:{int(sys.argv[2])}:{sys.argv[3]}"
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    v = r.get(key)
    if v is None:
        print("Key not in Redis. Fallback: run inference.py to compute live.")
    else:
        print(f"Cached prediction for {key} = {v.decode()}")
