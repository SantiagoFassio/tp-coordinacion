from common import message_protocol
import uuid

EOF_MESSAGE = "EOF"
DATA_MESSAGE = "DATA"

class MessageHandler:

    def __init__(self):
        self.query_id = str(uuid.uuid4())
        self.data_messages_handled = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.data_messages_handled += 1
        return message_protocol.internal.serialize([DATA_MESSAGE, self.query_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([EOF_MESSAGE, self.query_id, self.data_messages_handled])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        id_query, result = fields
        if id_query != self.query_id:
            return None
        
        return result
