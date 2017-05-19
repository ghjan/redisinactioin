import unittest
import redis
from config_redis import REDIS_CONFIG

from ch07_listing_source import *

class TestCh07(unittest.TestCase):
    content = 'this is some random content, look at how it is indexed.'

    def setUp(self):

        self.conn = redis.Redis(host=REDIS_CONFIG['host_r'], port=REDIS_CONFIG['port_r'], db=REDIS_CONFIG['db_r'], \
                                socket_connect_timeout=REDIS_CONFIG['timeout'], password=REDIS_CONFIG['password'])

        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()

    def test_index_document(self):
        print "We're tokenizing some content..."
        tokens = tokenize(self.content)
        print "Those tokens are:", tokens
        self.assertTrue(tokens)

        print "And now we are indexing that content..."
        r = index_document(self.conn, 'test', self.content)
        self.assertEquals(r, len(tokens))
        for t in tokens:
            self.assertEquals(self.conn.smembers('idx:' + t), set(['test']))

    def test_set_operations(self):
        index_document(self.conn, 'test', self.content)

        r = intersect(self.conn, ['content', 'indexed'])
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = intersect(self.conn, ['content', 'ignored'])
        self.assertEquals(self.conn.smembers('idx:' + r), set())

        r = union(self.conn, ['content', 'ignored'])
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = difference(self.conn, ['content', 'ignored'])
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = difference(self.conn, ['content', 'indexed'])
        self.assertEquals(self.conn.smembers('idx:' + r), set())

    def test_parse_query(self):
        query = 'test query without stopwords'
        self.assertEquals(parse(query), ([[x] for x in query.split()], []))

        query = 'test +query without -stopwords'
        self.assertEquals(parse(query), ([['test', 'query'], ['without']], ['stopwords']))

    def test_parse_and_search(self):
        print "And now we are testing search..."
        index_document(self.conn, 'test', self.content)

        r = parse_and_search(self.conn, 'content')
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = parse_and_search(self.conn, 'content indexed random')
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = parse_and_search(self.conn, 'content +indexed random')
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = parse_and_search(self.conn, 'content indexed +random')
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = parse_and_search(self.conn, 'content indexed -random')
        self.assertEquals(self.conn.smembers('idx:' + r), set())

        r = parse_and_search(self.conn, 'content indexed +random')
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        print "Which passed!"

    def test_search_with_sort(self):
        print "And now let's test searching with sorting..."

        index_document(self.conn, 'test', self.content)
        index_document(self.conn, 'test2', self.content)
        self.conn.hmset('kb:doc:test', {'updated': 12345, 'id': 10})
        self.conn.hmset('kb:doc:test2', {'updated': 54321, 'id': 1})

        r = search_and_sort(self.conn, "content")
        self.assertEquals(r[1], ['test2', 'test'])

        r = search_and_sort(self.conn, "content", sort='-id')
        self.assertEquals(r[1], ['test', 'test2'])
        print "Which passed!"

    def test_search_with_zsort(self):
        print "And now let's test searching with sorting via zset..."

        index_document(self.conn, 'test', self.content)
        index_document(self.conn, 'test2', self.content)
        self.conn.zadd('idx:sort:update', 'test', 12345, 'test2', 54321)
        self.conn.zadd('idx:sort:votes', 'test', 10, 'test2', 1)

        r = search_and_zsort(self.conn, "content", desc=False)
        self.assertEquals(r[1], ['test', 'test2'])

        r = search_and_zsort(self.conn, "content", update=0, vote=1, desc=False)
        self.assertEquals(r[1], ['test2', 'test'])
        print "Which passed!"

    def test_string_to_score(self):
        words = 'these are some words that will be sorted'.split()
        pairs = [(word, string_to_score(word)) for word in words]
        pairs2 = list(pairs)
        pairs.sort()
        pairs2.sort(key=lambda x: x[1])
        self.assertEquals(pairs, pairs2)

        words = 'these are some words that will be sorted'.split()
        pairs = [(word, string_to_score_generic(word, LOWER)) for word in words]
        pairs2 = list(pairs)
        pairs.sort()
        pairs2.sort(key=lambda x: x[1])
        self.assertEquals(pairs, pairs2)

        zadd_string(self.conn, 'key', 'test', 'value', test2='other')
        self.assertTrue(self.conn.zscore('key', 'test'), string_to_score('value'))
        self.assertTrue(self.conn.zscore('key', 'test2'), string_to_score('other'))

    def test_index_and_target_ads(self):
        index_ad(self.conn, '1', ['USA', 'CA'], self.content, 'cpc', .25)
        index_ad(self.conn, '2', ['USA', 'VA'], self.content + ' wooooo', 'cpc', .125)

        for i in xrange(100):
            ro = target_ads(self.conn, ['USA'], self.content)
        self.assertEquals(ro[1], '1')

        r = target_ads(self.conn, ['VA'], 'wooooo')
        self.assertEquals(r[1], '2')

        self.assertEquals(self.conn.zrange('idx:ad:value:', 0, -1, withscores=True), [('2', 0.125), ('1', 0.25)])
        self.assertEquals(self.conn.zrange('ad:base_value:', 0, -1, withscores=True), [('2', 0.125), ('1', 0.25)])

        record_click(self.conn, ro[0], ro[1])

        self.assertEquals(self.conn.zrange('idx:ad:value:', 0, -1, withscores=True), [('2', 0.125), ('1', 2.5)])
        self.assertEquals(self.conn.zrange('ad:base_value:', 0, -1, withscores=True), [('2', 0.125), ('1', 0.25)])

    def test_is_qualified_for_job(self):
        add_job(self.conn, 'test', ['q1', 'q2', 'q3'])
        self.assertTrue(is_qualified(self.conn, 'test', ['q1', 'q3', 'q2']))
        self.assertFalse(is_qualified(self.conn, 'test', ['q1', 'q2']))

    def test_index_and_find_jobs(self):
        index_job(self.conn, 'test1', ['q1', 'q2', 'q3'])
        index_job(self.conn, 'test2', ['q1', 'q3', 'q4'])
        index_job(self.conn, 'test3', ['q1', 'q3', 'q5'])

        self.assertEquals(find_jobs(self.conn, ['q1']), [])
        self.assertEquals(find_jobs(self.conn, ['q1', 'q3', 'q4']), ['test2'])
        self.assertEquals(find_jobs(self.conn, ['q1', 'q3', 'q5']), ['test3'])
        self.assertEquals(find_jobs(self.conn, ['q1', 'q2', 'q3', 'q4', 'q5']), ['test1', 'test2', 'test3'])


if __name__ == '__main__':
    unittest.main()
