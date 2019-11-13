import redis
import os
import logging
import time
import datetime
import asyncio
from typing import Set, List


LOGGER = logging.getLogger(__file__)
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = os.environ['REDIS_PORT']
REDIS_DB = os.environ['REDIS_DB']
REDIS_TTL = datetime.timedelta(days=float(os.environ.get('REDIS_TTL', '7')))
REDIS_TIME_RESOLUTION = datetime.timedelta(
    days=float(os.environ.get('REDIS_RESOLUTION', '1')))



class UserCache(object):
    def __init__(self, max_retry: int = 10):
        self.max_retry = max_retry
        self._redis = None
        
    @property
    def redis(self) -> redis.Redis:
        """The Redis Python client connection object
        """
        if not self._redis or not self._redis.ping():
            needs_client = True
            retry = 1
            while needs_client and retry <= self.max_retry:
                retry += 1
                self._redis = redis.Redis(
                    host=REDIS_HOST,
                    port=int(REDIS_PORT),
                    db=int(REDIS_DB),
                    encoding='utf-8',
                    decode_responses=True,
                )
                try:
                    if self._redis.ping():
                        needs_client = False
                    else:
                        time.sleep(1)
                except Exception as e:
                    LOGGER.error(
                        "Cache connection failed with {}".format(e))
                    time.sleep(1)
            if needs_client:
                raise Exception("Could not establish connection to cache")
        return self._redis

    @staticmethod
    def floor_dt(dt: datetime.datetime, res: datetime.timedelta) -> datetime.datetime:
        """Floor the provided datetime object to the closest interval
        as provided by the res timedelta object
        
        :param dt: the datetime to floor
        :type dt: datetime.datetime
        :param res: the timedelta defining the resolution for time flooring
        :type res: datetime.timedelta
        
        :returns: Datetime floored to closest resolution
        :rtype: datetime.datetime

        :Example:
        UserCache.floor_dt(
          datetime.datetime(2019,2,1,12,1,2),
          datetime.timedelta(days=1)
        ) -> datetime.datetime(2019,2,1)
        UserCache.floor_dt(
          datetime.datetime(2019,2,1,12,1,2),
          datetime.timedelta(days=0.5)
        ) -> datetime.datetime(2019,2,1,12)
        """
        # how many secs have passed
        nsecs = dt.hour*3600 + dt.minute*60 + dt.second + dt.microsecond*1e-6
        delta = nsecs % res.total_seconds()
        return dt - datetime.timedelta(seconds=delta)

    
    @staticmethod
    def unix_time(dt: datetime.datetime) -> int:
        """get seconds since epoch for datetime
        """
        epoch = datetime.datetime.utcfromtimestamp(0)
        return int((dt - epoch).total_seconds())

    async def query_cache(self, userId: str) -> Set[str]:
        """Query all valid time buckets for a given userId for cached interactions.
        
        :param userId: the userId
        :type userId: str
        
        :returns: Set of cached interactions
        :rtype: Set[str]
        """
        now = datetime.datetime.utcnow()
        result_set = set()

        for delta in range((REDIS_TTL.days // REDIS_TIME_RESOLUTION.days) + 1):
            bucket = self.unix_time(self.floor_dt(
                now - datetime.timedelta(days=delta), REDIS_TIME_RESOLUTION))
            key = "{}:{}".format(userId, bucket)
            s = self.redis.smembers(key)
            if s:
                result_set = result_set.union(s)
        return result_set

    async def add_to_cache(self, userId: str, interactionId: str) -> bool:
        """Cache interaction of a user.
        
        :param userId: the corresponding user Id of an interaction
        :type userId: str
        :param interactionId: the interaction ID to cache
        :type interactionId: str
        
        :returns: True
        :rtype: bool
        """
        bucket = self.unix_time(
            self.floor_dt(datetime.datetime.utcnow(),
            REDIS_TIME_RESOLUTION)
        )
        key = "{}:{}".format(userId, bucket)
        if type(interactionId) != list:
            interactionId = [interactionId]
        for v in interactionId:
            try:
                self.redis.sadd(key, v)
            except Exception as e:
                LOGGER.error(
                    "Failed to cache iteraction! {}:{} with error {}".format(
                    key, interactionId, e)
                )

        try:
            self.redis.expire(key, REDIS_TTL)
        except Exception as e:
            LOGGER.error(
                "Failed to extend TTL of key: {} with error {}".format(
                key, e)
            )

        return True

    async def user_cache(self, userId: str, interactionId: str, add_to_cache: bool) -> Set[str]:
        """Async interaction with the user cache.
        
        :param userId: the corresponding user Id of an interaction
        :type userId: str
        :param interactionId: the interaction ID to cache
        :type interactionId: str
        :param add_to_cache: Whether to cache interaction
        :type add_to_cache: bool
        
        :returns: Result set of cached interactions
        :rtype: Set[str]
        """
        if interactionId and add_to_cache:
            add_to_cache_future = asyncio.ensure_future(
                self.add_to_cache(userId, interactionId)
            )
        try:
            cache_response = await asyncio.wait_for(
                self.query_cache(userId), timeout=0.1
            )
        except asyncio.TimeoutError:
            cache_response = set()
        try:
            if interactionId and add_to_cache:
                _ = await asyncio.wait_for(
                    add_to_cache_future, timeout=0.1
                )
        except asyncio.TimeoutError:
            LOGGER.error(
                "Failed to cache iteraction! {}:{}".format(
                userId, interactionId)
            )
        return cache_response

    def __call__(self, userId: str, interactionId: str = None, add_to_cache: bool = False) -> List[str]:
        """Blocking call to the async user interaction cache.
        
        :param userId: the corresponding user Id of an interaction
        :type userId: str
        :param interactionId: the interaction ID to cache
        :type interactionId: str
        :param add_to_cache: Whether to cache interaction
        :type add_to_cache: bool
        
        :returns: List of cached user interactions
        :rtype: List[str]
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        if interactionId and type(interactionId) != list:
            interactionId = [interactionId]
        cache_response = loop.run_until_complete(
            self.user_cache(userId, interactionId, add_to_cache)
        )
        loop.close()
        if interactionId:
            return list(cache_response.union(set(interactionId)))
        else:
            return list(cache_response)
