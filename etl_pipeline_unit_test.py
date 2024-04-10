import unittest
from etl_pipeline import DataProducer, DataBuffer, DataConsumer
from datetime import datetime, timedelta

class TestDataProducer(unittest.TestCase):
    def test_generate_transaction(self):
        producer = DataProducer()
        transaction = producer.generate_transaction(1)
        self.assertIn(transaction['product'], ['apple', 'banana', 'strawberry', 'grape', 'pineapple'])
        self.assertTrue(1 <= transaction['amount'] <= 10)
        self.assertTrue(isinstance(transaction['timestamp'], datetime))

class TestDataBuffer(unittest.TestCase):
    def setUp(self):
        self.buffer = DataBuffer(10)

    def test_ingest_transaction(self):
        self.buffer.ingest_transaction({'id': 1})
        self.assertEqual(len(self.buffer.buffer), 1)

    def test_get_transaction(self):
        self.buffer.ingest_transaction({'id': 1})
        transaction = self.buffer.get_transaction()
        self.assertEqual(transaction['id'], 1)
        self.assertEqual(len(self.buffer.buffer), 0)

class TestDataConsumer(unittest.TestCase):
    def setUp(self):
        self.consumer = DataConsumer()

    def test_process_transaction(self):
        transaction = {'id': 1, 'product': 'apple', 'amount': 5, 'timestamp': datetime.now()}
        self.consumer.process_transaction(transaction)
        self.assertEqual(self.consumer.total_transactions, 1)
        self.assertEqual(self.consumer.total_sales, 5)
        self.assertEqual(self.consumer.sales_per_product['apple'], 5)


if __name__ == '__main__':
    unittest.main()
