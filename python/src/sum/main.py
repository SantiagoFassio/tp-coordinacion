import os
import logging
import threading
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

EOF_MESSAGE = "EOF"
DATA_MESSAGE = "DATA"
COUNT_TOKEN_MESSAGE = "COUNT"
FLUSH_ORDER_MESSAGE = "FLUSH"

class SumFilter:
    def __init__(self):
        #lock
        self.lock = threading.Lock()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        # Control ring: DOs instancias por cada sum
        # - instancia de entrada de datos del SUM anterior
        # - instancia de salida de datos al SUM siguiente
        self.personal_control_key = f"{SUM_PREFIX}_{ID}"
        self.next_control_key = f"{SUM_PREFIX}_{(ID + 1) % SUM_AMOUNT}"

        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [self.personal_control_key]
        )
        self.control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [self.next_control_key]
        )
        
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_query = {} # query_id -> fruit
        self.local_count_by_query = {} # query_id -> count of data messages received
        self.last_contributed_by_query = {} # query_id -> id of last sum that contributed to the count
        self.expected_total_by_query = {} # query_id -> total count expected to receive before flushing (in EOF message)
        self.token_started_by_query = set() # query_id -> token already started for the query
        self.flushed_by_query = set() # query_id -> already flushed the query
        self.closed_by_query = set() # query_id -> already closed the query, no more data messages should be processed

    def _routing_key_for_fruit(self, fruit):
        idx = zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT
        return f"{AGGREGATION_PREFIX}_{idx}"
    
    def _publish_control_message(self, message):
        self.control_output.send(message)

    def _publish_count_token(
            self, query_id, expected_total, accumulated_count, dirty, origin_id
    ):
        token = message_protocol.internal.serialize(
            [
                COUNT_TOKEN_MESSAGE,
                query_id,
                expected_total,
                accumulated_count,
                dirty,
                origin_id,
            ]
        )
        self._publish_control_message(token)

    def _publish_flush_order(self, query_id, origin_id):
        msg = message_protocol.internal.serialize(
            [
                FLUSH_ORDER_MESSAGE,
                query_id,
                origin_id
            ]
        )
        self._publish_control_message(msg)

    def _contribute_local_count(self, query_id):
        local_count = self.local_count_by_query.get(query_id, 0)
        last = self.last_contributed_by_query.get(query_id, 0)
        delta = local_count - last
        if delta != 0:
            self.last_contributed_by_query[query_id] = local_count
        return delta
    
    def _start_token(self, query_id, expected_total):
        local_count = self.local_count_by_query.get(query_id, 0)
        self.last_contributed_by_query[query_id] = local_count
        self._publish_count_token(
            query_id, expected_total, local_count, False, ID
        )
        self.token_started_by_query.add(query_id)
    
    def _flush_query(self, query_id):
        if query_id in self.flushed_by_query:
            return
        
        logging.info(f"flushing query {query_id}")

        data = self.amount_by_query.get(query_id, {})
        for final_fruit_item in data.values():
            routing_key = self._routing_key_for_fruit(final_fruit_item.fruit)
            shard_idx = int(routing_key.split("_", 1)[-1])

            self.data_output_exchanges[shard_idx].send(
                message_protocol.internal.serialize(
                    [query_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
        
        for exchange in self.data_output_exchanges:
            exchange.send(message_protocol.internal.serialize([query_id]))

        self.flushed_by_query.add(query_id)

        self.amount_by_query.pop(query_id, None)
        self.local_count_by_query.pop(query_id, None)
        self.last_contributed_by_query.pop(query_id, None)
        self.expected_total_by_query.pop(query_id, None)
    
    def _cleanup_query(self, query_id):
        self.amount_by_query.pop(query_id, None)
        self.local_count_by_query.pop(query_id, None)
        self.last_contributed_by_query.pop(query_id, None)
        self.expected_total_by_query.pop(query_id, None)
        self.token_started_by_query.discard(query_id)

    def _process_data(self, query_id, fruit, amount):
        #logging.info(f"Process data")

        if query_id in self.flushed_by_query or query_id in self.closed_by_query:
            logging.info(f"Query {query_id} already flushed or closed, This should not happen, ignoring data message")
            return

        if query_id not in self.amount_by_query:
            self.amount_by_query[query_id] = {}
            self.local_count_by_query[query_id] = 0
            self.last_contributed_by_query[query_id] = 0

        current = self.amount_by_query[query_id]
        current[fruit] = current.get(fruit, fruit_item.FruitItem(fruit, 0)) + \
                fruit_item.FruitItem(fruit, int(amount))
        
        self.amount_by_query[query_id] = current
        self.local_count_by_query[query_id] += 1

    def _process_eof(self, query_id, messages_handled):
        logging.info(f"EOF. Broadcasting data messages")

        if query_id in self.flushed_by_query or query_id in self.closed_by_query:
            logging.info(f"Query {query_id} already flushed or closed, This should not happen, ignoring EOF message")
            return
        
        expected_total = int(messages_handled)
        self.expected_total_by_query[query_id] = expected_total

        if query_id not in self.token_started_by_query:
            self._start_token(query_id, expected_total)

    def _process_count_token(
        self, query_id, expected_total, accumulated_count, dirty, origin_id
    ):
        logging.info(f"Process count token for query {query_id} with accumulated count {accumulated_count} and expected total {expected_total}")

        if query_id in self.flushed_by_query or query_id in self.closed_by_query:
            logging.info(f"Query {query_id} already flushed or closed, This should not happen, ignoring count token")
            return

        delta = self._contribute_local_count(query_id)
        if delta != 0:
            accumulated_count += delta
            dirty = True

        if origin_id == ID:
            if accumulated_count == expected_total and not dirty:
                logging.info(f"Token completed a full round and counts match, flushing query {query_id}")
                self._publish_flush_order(query_id, ID)
            else:
                dirty = False
                self._publish_count_token(
                    query_id, expected_total, accumulated_count, dirty, ID
                )
        else:
            self._publish_count_token(
                query_id, expected_total, accumulated_count, dirty, origin_id
            )
    
    def _process_flush_order(self, query_id, origin_id):
        if origin_id == ID:
            self._flush_query(query_id)
            self.closed_by_query.add(query_id)
            return

        if query_id in self.flushed_by_query or query_id in self.closed_by_query:
            logging.info(f"Query {query_id} already flushed or closed, This should not happen, ignoring flush order")
            return

        self._flush_query(query_id)
        self.closed_by_query.add(query_id)
        self._cleanup_query(query_id)

        self._publish_flush_order(query_id, origin_id)

    def process_data_messsage(self, message, ack, nack):
        try:

            fields = message_protocol.internal.deserialize(message)
            msg_type = fields[0]

            with self.lock:
                if msg_type == DATA_MESSAGE:
                    _, query_id, fruit, amount = fields
                    self._process_data(query_id, fruit, amount)
                elif msg_type == EOF_MESSAGE:
                    _, query_id, messages_handled = fields
                    self._process_eof(query_id, messages_handled)

            ack()
        except Exception as e:
            logging.error(f"Error processing data message: {e}")
            nack()
    
    def process_control_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            msg_type = fields[0]

            with self.lock:
                if msg_type == COUNT_TOKEN_MESSAGE:
                    _, query_id, expected_total, accumulated_count, dirty, origin_id = fields
                    self._process_count_token(
                        query_id, expected_total, accumulated_count, dirty, origin_id
                    )
                elif msg_type == FLUSH_ORDER_MESSAGE:
                    _, query_id, origin_id = fields
                    self._process_flush_order(query_id, origin_id)

            ack()
        except Exception as e:
            logging.error(f"Error processing control message: {e}")
            nack()

    def start(self):
        control_thread = threading.Thread(
            target=self.control_input.start_consuming,
            args=(self.process_control_message,),
            daemon=True
        )
        control_thread.start()

        self.input_queue.start_consuming(self.process_data_messsage)

    def close(self):
        try:
            self.input_queue.close()
        except Exception:
            pass

        try:
            self.control_input.close()
        except Exception:
            pass

        try:
            self.control_output.close()
        except Exception:
            pass

        for exchange in self.data_output_exchanges:
            try:
                exchange.close()
            except Exception:
                pass

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
