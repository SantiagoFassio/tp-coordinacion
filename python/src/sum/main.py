import os
import logging
import threading
import signal

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
    '''
    SumFilter is a filter that receives data messages from the client, 
    processes them to keep track of the count of each fruit for each query, 
    and synchronizes with other SumFilter instances in a ring architecture to 
    ensure all data messages for a query have been received before 
    flushing the results to the aggregation layer.
    '''
    def __init__(self):
        self.lock = threading.Lock()
        self.should_stop = False
        self.control_thread = None

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

    def _hash_fruit_for_key(self, fruit):
        '''
        Hashing function to determine the exchange a fruit will be sent to.
        A fruit will always be sent to the same exchange.
        '''
        h = 0
        for c in fruit:
            h = (h * 31 + ord(c)) % AGGREGATION_AMOUNT
        return h
    
    def _publish_control_message(self, message):
        '''
        Publishes a control message to the next sum instance in the ring.
        Control messages are used for synchronization between sum instances, 
        such as count tokens and flush orders.
        '''
        self.control_output.send(message)

    def _publish_count_token(
            self, query_id, expected_total, accumulated_count, dirty, origin_id
    ):
        '''
        Creates and publishes a count token message to the next sum instance in the ring.
        Count tokens are used to keep track of how many data messages have been received for a query
        accross all sum instances.
        '''
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
        '''
        Creates and publishes a flush order message to the next sum instance in the ring.
        Flush order messages are used to signal all sum instances to flush the results of a query
        to the aggregation layer.
        '''
        msg = message_protocol.internal.serialize(
            [
                FLUSH_ORDER_MESSAGE,
                query_id,
                origin_id
            ]
        )
        self._publish_control_message(msg)

    def _contribute_local_count(self, query_id):
        '''
        Contributes the local count of data messages received for a query to the count token.
        If the local count has changed since the last contribution, it returns the delta to be
        added to the accumulated count in the token, and updates the last contributed count.
        '''
        local_count = self.local_count_by_query.get(query_id, 0)
        last = self.last_contributed_by_query.get(query_id, 0)
        delta = local_count - last
        if delta != 0:
            self.last_contributed_by_query[query_id] = local_count
        return delta
    
    def _start_token(self, query_id, expected_total):
        '''
        A Sum designates itself as the Tokn Master for a query as it receives the EOF message
        from the previous stage. The Token Master is responsible for starting the count token and
        for publishing flush orders when the token completes a full round and the accumulated count
        matches the expected total.
        '''
        local_count = self.local_count_by_query.get(query_id, 0)
        self.last_contributed_by_query[query_id] = local_count
        self._publish_count_token(
            query_id, expected_total, local_count, False, ID
        )
        self.token_started_by_query.add(query_id)
    
    def _flush_query(self, query_id):
        '''
        When a flush order is received, the Sum flushes the results of the query to the aggregation layer.
        It also deletes all the state related to the query and adds the query to the flushed set 
        to ignore any bugs or messages that might arrive related to the flushed query.
        These messages are not expected to arrive, but this is a safety measure to avoid errors.
        '''
        if query_id in self.flushed_by_query:
            return
        
        logging.info(f"flushing query {query_id}")

        data = self.amount_by_query.get(query_id, {})
        for final_fruit_item in data.values():
            key = self._hash_fruit_for_key(final_fruit_item.fruit)

            self.data_output_exchanges[key].send(
                message_protocol.internal.serialize(
                    [DATA_MESSAGE, query_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        self.flushed_by_query.add(query_id)

        self.amount_by_query.pop(query_id, None)
        self.local_count_by_query.pop(query_id, None)
        self.last_contributed_by_query.pop(query_id, None)
        self.expected_total_by_query.pop(query_id, None)
    
    def _flush_eof(self, query_id):
        '''
        The Token Master, when recieving the flush order back, assumes all information has been published
        to the aggregation layer and broadcasts an EOF message to signal the end of the query results. 
        This is to inform the aggregation level all information on that query has been sent.
        '''
        eof_message = message_protocol.internal.serialize([EOF_MESSAGE, query_id])
        for exchange in self.data_output_exchanges:
            exchange.send(eof_message)
    
    def _cleanup_query(self, query_id):
        '''
        Inclusion of a cleanup function to remove all state related to a query.
        '''
        self.amount_by_query.pop(query_id, None)
        self.local_count_by_query.pop(query_id, None)
        self.last_contributed_by_query.pop(query_id, None)
        self.expected_total_by_query.pop(query_id, None)
        self.token_started_by_query.discard(query_id)

    def _process_data(self, query_id, fruit, amount):
        '''
        Process a data message sent by the client.
        It updates the local state of this Sum instance with the new information, 
        and increments the local count of messages received for the query for sync purposes.
        '''

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
        '''
        Process an EOF message sent by the previous stage, indicating that all data messages for a query have been sent.
        The Sum instance that receives the EOF message first designates itself as the Token Master for the
        query, and is responsible for starting the count token and for publishing flush orders when the token
        completes a full round and the accumulated count matches the expected total.
        '''
        logging.info(f"EOF received for query {query_id} with messages handled {messages_handled}")

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
        '''
        Process a count token message received from the previous Sum instance in the ring.
        The count token carries the accumulated count of data messages received for a query across all Sum instances
        as it circulates the ring. Each Sum instance contributes its local count to the token, and if the token
        completes a full round and the accumulated count matches the expected total, 
        the Token Master publishes a flush order for all other SUM instances.
        '''
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
        '''
        Process a flush order message received from the previous Sum instance in the ring.
        When a flush order is received, the Sum instance flushes the results of the query to
        the aggregation layer, and if it is the Token Master, it also broadcasts an EOF message 
        to signal the end of the query results.
        '''
        if origin_id == ID:
            self._flush_query(query_id)
            self._flush_eof(query_id)
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
        '''
        When a data message is received from the client, it is processed to 
        update the local state of the Sum instance.
        '''
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
        '''
        When a control message is received from the previous Sum instance in the ring, it is processed to
        update the synchronization state of the Sum instance, and to contribute to the count token or to
        flush the query results when a flush order is received.
        '''
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
        '''
        Starts the Sum filter by starting the control thread to consume control messages 
        from the previous Sum instance in the ring,
        and starting to consume data messages from the client.
        '''
        self.control_thread = threading.Thread(
            target=self.control_input.start_consuming,
            args=(self.process_control_message,)
        )
        self.control_thread.start()
        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            self.handle_sigterm()
            if self.control_thread is not None:
                self.control_thread.join(timeout = 5)
            self.close()

    def handle_sigterm(self):
        '''
        Handles the sigterm signal for graceful shutdown.
        '''
        if self.should_stop:
            return
        logging.info("Received SIGTERM, stopping gracefully...")
        self.should_stop = True
        self.input_queue.stop_consuming()
        self.control_input.stop_consuming()
        self.close()

    def close(self):
        '''
        Closes all queues and exchanges used by the Sum filter.
        '''
        self.input_queue.close()
        self.control_input.close()
        self.control_output.close()
        for exchange in self.data_output_exchanges:
            try:
                exchange.close()
            except Exception:
                pass

def main():
    '''
    Main function to start the Sum filter. It sets up logging, creates an instance of the SumFilter class,
    '''
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: sum_filter.handle_sigterm())
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
