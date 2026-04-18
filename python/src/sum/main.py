import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_query = {}

    def _process_data(self, query_id, fruit, amount):
        logging.info(f"Process data")

        if query_id not in self.amount_by_query:
            self.amount_by_query[query_id] = {}

        current = self.amount_by_query[query_id]

        current[fruit] = current.get(fruit, fruit_item.FruitItem(fruit, 0)) + \
                fruit_item.FruitItem(fruit, int(amount))
        
        self.amount_by_query[query_id] = current

    def _process_eof(self, query_id):
        logging.info(f"Broadcasting data messages")

        data = self.amount_by_query.get(query_id, {})

        for final_fruit_item in data.values():
            for exchange in self.data_output_exchanges:
                exchange.send(
                    message_protocol.internal.serialize(
                        [query_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message")

        for exchange in self.data_output_exchanges:
            exchange.send(message_protocol.internal.serialize([query_id]))
        
        del self.amount_by_query[query_id]


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            query_id, fruit, amount = fields
            self._process_data(query_id, fruit, amount)
        elif len(fields) == 1:
            query_id = fields[0]
            self._process_eof(query_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
