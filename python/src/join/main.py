import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.messages_by_query = {}
        self.partial_top_by_query = {}
        self.closed_queries = set()

    def _process_top(self, query_id, fruit_top):


        if query_id in self.closed_queries:
            logging.info(f"Query {query_id} already closed, This should not happen, ignoring top message")
            return
        
        if query_id not in self.messages_by_query:
            self.partial_top_by_query[query_id] = {}
            self.messages_by_query[query_id] = 0

        for fruit, amount in fruit_top:
            self.partial_top_by_query[query_id][fruit] = amount
        
        self.messages_by_query[query_id] += 1

        if self.messages_by_query[query_id] < AGGREGATION_AMOUNT:
            logging.info(f"Received partial top for {query_id}, waiting for more messages")
            return
        
        logging.info(f"Received all partial tops for {query_id}, calculating final top and sending to output queue")
        
        self.closed_queries.add(query_id)

        fruit_counts = self.partial_top_by_query[query_id]
        fruit_top = sorted(fruit_counts.items(), key=lambda x: x[1], reverse=True)[:TOP_SIZE]
        self.output_queue.send(message_protocol.internal.serialize([query_id, fruit_top]))

    def process_messsage(self, message, ack, nack):
        query_id, fruit_top = message_protocol.internal.deserialize(message)
        logging.info(f"Received top from {query_id}")
        self._process_top(query_id, fruit_top)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
