import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import unittest
from web3_data_center.clients.opensearch_client import OpenSearchClient

# ... rest of the test code remains the same ...

class TestOpenSearchClient(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.client = OpenSearchClient()

    def tearDown(self):
        self.loop.run_until_complete(self.client.close())

    def test_connection(self):
        connected = self.loop.run_until_complete(self.client.test_connection())
        self.assertTrue(connected, "Failed to connect to OpenSearch")

    def test_index_exists(self):
        # Replace 'test_index' with an index name that should exist in your OpenSearch setup
        exists = self.loop.run_until_complete(self.client.check_index_exists('eth_block'))
        self.assertTrue(exists, "Test index does not exist")

    def test_search_logs(self):
        # Replace these parameters with values suitable for your OpenSearch setup
        index = 'eth_block'
        start_block = 20850000
        end_block = 20860000
        event_topics = ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822']
        
        logs = self.loop.run_until_complete(self.client.search_logs(index, start_block, end_block, event_topics))
        self.assertIsInstance(logs, list, "search_logs should return a list")
        self.assertGreater(len(logs), 0, "No logs found in the specified range")

    def test_query_building(self):
        start_block = 1000000
        end_block = 1000100
        event_topics = ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822']
        size = 100

        query = OpenSearchClient._build_query(start_block, end_block, event_topics, size)
        
        self.assertIn('query', query, "Query should contain a 'query' field")
        self.assertIn('bool', query['query'], "Query should contain a 'bool' field")
        self.assertIn('must', query['query']['bool'], "Query should contain a 'must' field")
        self.assertEqual(len(query['query']['bool']['must']), 2, "Query should contain two 'must' conditions")
        self.assertEqual(query['size'], size, "Query size should match the input size")

if __name__ == '__main__':
    unittest.main()