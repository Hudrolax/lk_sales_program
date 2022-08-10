import redis
import json
from json import JSONEncoder
import datetime


# subclass JSONEncoder
class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


class UpgradedRedis(redis.Redis):
    def set_dict(self, key: str, _dict: dict, **kwargs) -> bool:
        json_string = json.dumps(_dict, ensure_ascii=False, cls=DateTimeEncoder).encode('utf-8')
        return self.set(key, json_string, **kwargs)

    def get_dict(self, key: str) -> dict:
        return json.loads(self.get(key))
