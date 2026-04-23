from common import message_protocol
import uuid

EOF_MESSAGE = "EOF"
DATA_MESSAGE = "DATA"

class MessageHandler:
    '''
    MessageHandler is responsible for serializing data and EOF messages to be sent to the Aggregation layer,
    and deserializing the result messages received from the Joining layer.
    '''

    def __init__(self):
        self.query_id = str(uuid.uuid4())
        self.data_messages_handled = 0
    
    def serialize_data_message(self, message):
        '''
        Serializes a data message for the Sum layer. Also keeps track of the
        number of data messages handled for the query.
        '''
        [fruit, amount] = message
        self.data_messages_handled += 1
        return message_protocol.internal.serialize([DATA_MESSAGE, self.query_id, fruit, amount])

    def serialize_eof_message(self, message):
        '''
        Serializes an EOF message for the Sum layer.
        '''
        return message_protocol.internal.serialize([EOF_MESSAGE, self.query_id, self.data_messages_handled])

    def deserialize_result_message(self, message):
        '''
        Deserializes a result message received from the Joining layer, and returns the query id and the result.
        '''
        fields = message_protocol.internal.deserialize(message)

        id_query, result = fields
        if id_query != self.query_id:
            return None
        
        return result
