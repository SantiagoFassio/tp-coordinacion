import json


def serialize(message):
    '''
    Serializes a message to be sent through the message queues.
    The message is serialized as a JSON string and encoded to bytes.
    '''
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    '''
    Deserializes a message received from the message queues.
    The message is decoded from bytes and deserialized from a JSON string.
    '''
    return json.loads(message.decode("utf-8"))
