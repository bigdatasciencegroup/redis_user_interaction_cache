import unittest
from unittest import mock
import datetime
from cache import cache


class TestRedisCache(unittest.TestCase):
    def setUp(self):
        self.c = cache.UserCache()

    def tearDown(self):
        self.c.redis.flushdb()

    def test_connection(self):
        self.assertTrue(self.c.redis.ping())

    def test_calling_cache(self):
        r = self.c("u1", "1", True)
        self.assertCountEqual(r, ["1"])
        r = self.c("u1", "2", True)
        self.assertCountEqual(r, ["1", "2"])
        r = self.c("u1", "1", True)
        self.assertCountEqual(r, ["1", "2"])
        r = self.c("u1")
        self.assertCountEqual(r, ["1", "2"])
        r = self.c("u2")
        self.assertCountEqual(r, [])
        r = self.c("u2", "1", True)
        self.assertCountEqual(r, ["1"])
        r = self.c("u2", "2", False)
        self.assertCountEqual(r, ["1", "2"])
        r = self.c("u2")
        self.assertCountEqual(r, ["1"])

    def test_time_flooring(self):
        self.assertEqual(
            self.c.floor_dt(
                datetime.datetime(2019, 2, 1, 12, 1, 2), datetime.timedelta(days=1)
            ),
            datetime.datetime(2019, 2, 1),
        )
        self.assertEqual(
            self.c.floor_dt(
                datetime.datetime(2019, 2, 1, 12, 1, 2), datetime.timedelta(hours=1)
            ),
            datetime.datetime(2019, 2, 1, 12),
        )
        self.assertEqual(
            self.c.floor_dt(
                datetime.datetime(2019, 2, 1, 12, 1, 2), datetime.timedelta(days=0.5)
            ),
            datetime.datetime(2019, 2, 1, 12),
        )
        self.assertEqual(
            self.c.floor_dt(
                datetime.datetime(2019, 2, 1, 12, 1, 2), datetime.timedelta(days=100)
            ),
            datetime.datetime(2019, 2, 1),
        )

    @mock.patch.object(cache, "datetime", mock.Mock(wraps=datetime))
    def test_time_bucketing(self):
        cache.datetime.datetime.utcnow.return_value = datetime.datetime(
            2019, 11, 11, 9, 0, 0
        )
        _ = self.c("u1", "1", True)
        r = self.c("u1")
        self.assertCountEqual(r, ["1"])

        cache.datetime.datetime.utcnow.return_value = datetime.datetime(
            2019, 11, 11, 18, 0, 0
        )
        r = self.c("u1")
        self.assertCountEqual(r, ["1"])

        cache.datetime.datetime.utcnow.return_value = datetime.datetime(
            2019, 11, 12, 9, 0, 0
        )
        r = self.c("u1")
        self.assertCountEqual(r, ["1"])

        cache.datetime.datetime.utcnow.return_value = datetime.datetime(
            2019, 11, 13, 9, 0, 0
        )
        r = self.c("u1")
        self.assertCountEqual(r, [])


if __name__ == "__main__":
    unittest.main()
