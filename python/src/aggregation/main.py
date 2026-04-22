import os
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

DATA_MESSAGE = "DATA"
EOF_MESSAGE = "EOF"

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_counts_by_query = {}
        self.closed_queries = set()

    def _process_data(self, query_id, fruit, amount):
        logging.info(f"Processing data message for {query_id}")

        if query_id in self.closed_queries:
            logging.info(f"Query {query_id} already closed, This should not happen, ignoring data message")
            return

        if query_id not in self.fruit_counts_by_query:
            self.fruit_counts_by_query[query_id] = {}
        self.fruit_counts_by_query[query_id][fruit] = self.fruit_counts_by_query[query_id].get(fruit, 0) + amount

    def _process_eof(self, query_id):

        if query_id in self.closed_queries:
            logging.info(f"Query {query_id} already closed, This should not happen, ignoring EOF message")
            return
        
        logging.info(f"Received EOF for {query_id}, calculating partial top and sending to output queue")

        self.closed_queries.add(query_id)

        fruit_counts = self.fruit_counts_by_query.get(query_id, {})
        fruit_top = sorted(fruit_counts.items(), key=lambda x: x[1], reverse=True)[:TOP_SIZE]
        
        self.output_queue.send(message_protocol.internal.serialize([query_id, fruit_top]))
        del self.fruit_counts_by_query[query_id]

    def process_messsage(self, message, ack, nack):

        fields = message_protocol.internal.deserialize(message)
        msg_type = fields[0]

        if msg_type == DATA_MESSAGE:
            _, query_id, fruit, amount = fields
            self._process_data(query_id, fruit, amount)
        if msg_type == EOF_MESSAGE:
            _, query_id = fields
            self._process_eof(query_id)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
