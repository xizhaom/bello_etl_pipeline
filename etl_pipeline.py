import random
import time
from collections import deque
from datetime import datetime, timedelta

# Data Producer
class DataProducer:
    def __init__(self):
        self.products = ['apple', 'banana', 'strawberry', 'grape', 'pineapple']
    
    def generate_transaction(self, transaction_id):
        product = random.choice(self.products)
        amount = random.randint(1, 10)
        timestamp = datetime.now() - timedelta(minutes=random.randint(0, 30))
        return {'id': transaction_id, 'product': product, 'amount': amount, 'timestamp': timestamp}

# Data Buffer
class DataBuffer:
    def __init__(self, max_size):
        self.buffer = deque(maxlen=max_size)
    
    def ingest_transaction(self, transaction):
        if len(self.buffer) == self.buffer.maxlen:
            print("Buffer is full. Dropping the oldest transaction.")
        self.buffer.append(transaction)
    
    def get_transaction(self):
        if self.buffer:
            return self.buffer.popleft()
        return None

# Data Consumer
class DataConsumer:
    def __init__(self):
        self.total_transactions = 0
        self.total_sales = 0
        self.sales_per_product = {}
        self.transactions = []
    
    def process_transaction(self, transaction):
        self.total_transactions += 1
        self.total_sales += transaction['amount']
        product = transaction['product']
        if product not in self.sales_per_product:
            self.sales_per_product[product] = 0
        self.sales_per_product[product] += transaction['amount']
        self.transactions.append(transaction)  
    
    def calculate_time_based_metrics(self, minutes):
        cutoff = datetime.now() - timedelta(minutes=minutes)
        filtered_transactions = [t for t in self.transactions if t['timestamp'] > cutoff]
        sales = sum(t['amount'] for t in filtered_transactions)
        print(f"Sales in the last {minutes} minutes: {sales}")
    
    def log_metrics(self):
        print(f"Total transactions: {self.total_transactions}, Total sales: {self.total_sales}, Sales per product: {self.sales_per_product}")

# Main Program
def main():
    buffer = DataBuffer(max_size=10)
    consumer = DataConsumer()
    producer = DataProducer()
    transaction_id = 0

    for _ in range(20):  
        transaction = producer.generate_transaction(transaction_id)
        print(f"Generated transaction {transaction_id}: {transaction}")
        buffer.ingest_transaction(transaction)
        transaction_id += 1

        transaction = buffer.get_transaction()
        if transaction:
            consumer.process_transaction(transaction)

        if transaction_id % 5 == 0:
            consumer.log_metrics()
            for minutes in [5, 10, 15]:
                consumer.calculate_time_based_metrics(minutes)

        time.sleep(1)  

if __name__ == '__main__':
    main()
