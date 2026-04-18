from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.query_id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.query_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.query_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        id_query, result = fields
        if id_query != self.query_id:
            return None
        
        return result
