import json

def safe_float(x, default=0.0):
    if x is None:
        return default
    try:
        return float(x)
    except:
        return default

def safe_str(x, default=""):
    if x is None:
        return default
    return str(x)

def parse_feature(data):
    feature_names = data["metadata"]["feature_names"]
    results = data["results"]
    parsed = {}
    for i, name in enumerate(feature_names):
        parsed[name] = results[i]["values"][0]
    return parsed

def get_item_metadata(item_id: str, redis_client):
    data = redis_client.get(f"item:{item_id}")
    if not data:
        raise ValueError(f"No metadata found for item {item_id}")
    return json.loads(data)
