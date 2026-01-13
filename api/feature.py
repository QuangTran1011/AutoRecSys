from loguru import logger
from api.utils import safe_float, safe_str, parse_feature, get_item_metadata
import httpx


async def get_item_feature(item_id: str, FEAST_ONLINE_SERVER_HOST, FEAST_ONLINE_SERVER_PORT):
    url = f"http://{FEAST_ONLINE_SERVER_HOST}:{FEAST_ONLINE_SERVER_PORT}/get-online-features"
    payload = {
        "features": [
            "parent_asin_rating_stats:parent_asin_rating_cnt_365d",
            "parent_asin_rating_stats:parent_asin_rating_avg_prev_rating_365d",
            "parent_asin_rating_stats:parent_asin_rating_cnt_90d",
            "parent_asin_rating_stats:parent_asin_rating_avg_prev_rating_90d",
            "parent_asin_rating_stats:parent_asin_rating_cnt_30d",
            "parent_asin_rating_stats:parent_asin_rating_avg_prev_rating_30d",
            "parent_asin_rating_stats:parent_asin_rating_cnt_7d",
            "parent_asin_rating_stats:parent_asin_rating_avg_prev_rating_7d",
        ],
        "entities": {"parent_asin": [item_id]}
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()

async def build_payload(user_id: str, item_ids: list, redis_client, FEAST_ONLINE_SERVER_HOST, FEAST_ONLINE_SERVER_PORT):
    # Lấy user feature
    raw_user_feat = await get_user_feature(user_id, FEAST_ONLINE_SERVER_HOST, FEAST_ONLINE_SERVER_PORT)
    user_feat = parse_feature(raw_user_feat)
    item_sequence = user_feat["user_rating_list_10_recent_asin"]
    item_sequence_ts = user_feat["user_rating_list_10_recent_asin_timestamp"]

    main_category, categories, price = [], [], []
    parent_asin_rating_cnt_365d, parent_asin_rating_avg_prev_rating_365d = [], []
    parent_asin_rating_cnt_90d, parent_asin_rating_avg_prev_rating_90d = [], []
    parent_asin_rating_cnt_30d, parent_asin_rating_avg_prev_rating_30d = [], []
    parent_asin_rating_cnt_7d, parent_asin_rating_avg_prev_rating_7d = [], []
    parent_asin_list = []

    for item_id in item_ids:
        raw_feat = await get_item_feature(item_id, FEAST_ONLINE_SERVER_HOST, FEAST_ONLINE_SERVER_PORT)
        feat = parse_feature(raw_feat)
        meta = get_item_metadata(item_id, redis_client)

        main_category.append(safe_str(meta.get("main_category")))
        categories.append(safe_str("__".join(meta.get("categories", []))))
        price.append(safe_str(meta.get("price"), "0"))

        parent_asin_rating_cnt_365d.append(safe_float(feat.get("parent_asin_rating_cnt_365d")))
        parent_asin_rating_avg_prev_rating_365d.append(safe_float(feat.get("parent_asin_rating_avg_prev_rating_365d")))
        parent_asin_rating_cnt_90d.append(safe_float(feat.get("parent_asin_rating_cnt_90d")))
        parent_asin_rating_avg_prev_rating_90d.append(safe_float(feat.get("parent_asin_rating_avg_prev_rating_90d")))
        parent_asin_rating_cnt_30d.append(safe_float(feat.get("parent_asin_rating_cnt_30d")))
        parent_asin_rating_avg_prev_rating_30d.append(safe_float(feat.get("parent_asin_rating_avg_prev_rating_30d")))
        parent_asin_rating_cnt_7d.append(safe_float(feat.get("parent_asin_rating_cnt_7d")))
        parent_asin_rating_avg_prev_rating_7d.append(safe_float(feat.get("parent_asin_rating_avg_prev_rating_7d")))

        parent_asin_list.append(item_id)

    payload = {
        "input_data": {
            "user_id": [user_id] * len(item_ids),
            "item_sequence": [item_sequence] * len(item_ids),
            "item_sequence_ts": [item_sequence_ts] * len(item_ids),
            "main_category": main_category,
            "categories": categories,
            "price": price,
            "parent_asin_rating_cnt_365d": parent_asin_rating_cnt_365d,
            "parent_asin_rating_avg_prev_rating_365d": parent_asin_rating_avg_prev_rating_365d,
            "parent_asin_rating_cnt_90d": parent_asin_rating_cnt_90d,
            "parent_asin_rating_avg_prev_rating_90d": parent_asin_rating_avg_prev_rating_90d,
            "parent_asin_rating_cnt_30d": parent_asin_rating_cnt_30d,
            "parent_asin_rating_avg_prev_rating_30d": parent_asin_rating_avg_prev_rating_30d,
            "parent_asin_rating_cnt_7d": parent_asin_rating_cnt_7d,
            "parent_asin_rating_avg_prev_rating_7d": parent_asin_rating_avg_prev_rating_7d,
            "parent_asin": parent_asin_list
        }
    }
    return payload
