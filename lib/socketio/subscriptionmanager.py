import re


class SubscriptionManager:
    _subscribers = []

    @classmethod
    async def add_subscriber(cls, sid, regex):
        current_subscribers = [sub.get('sid') for sub in cls._subscribers]
        if sid not in current_subscribers:
            cls._subscribers.append({
                'sid': sid,
                'regex': regex
            })

    @classmethod
    async def remove_subscriber(cls, sid):
        current_subscribers = [sub.get('sid') for sub in cls._subscribers]
        if sid in current_subscribers:
            sub_to_remove = None
            for sub in cls._subscribers:
                if sub.get('sid') == sid:
                    sub_to_remove = sub
                    break
            cls._subscribers.remove(sub_to_remove)

    @classmethod
    async def get_all_filtered_subscriptions(cls):
        return [sub for sub in cls._subscribers]

    @classmethod
    async def get_subscriptions_for_series_name(cls, series_name):
        subs = []

        for sub in cls._subscribers:
            pattern = re.compile(sub.regex)
            if pattern.match(series_name):
                subs.append(sub.sid)

        return subs
