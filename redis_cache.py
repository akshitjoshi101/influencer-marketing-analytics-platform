import json
import os
import redis


REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://localhost:6379/0"
)

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True
)


def get_cache(key):
    data = redis_client.get(key)

    if data is None:
        return None

    return json.loads(data)


def set_cache(key, data, ttl=60):
    redis_client.setex(
        key,
        ttl,
        json.dumps(data)
    )


def delete_cache(key):
    redis_client.delete(key)

def clear_analytics_cache():
    # Invalidate existing analytics and the new user-specific statistics caches.
    keys = list(redis_client.scan_iter(match="analytics:*"))
    if keys:
        redis_client.delete(*keys)
