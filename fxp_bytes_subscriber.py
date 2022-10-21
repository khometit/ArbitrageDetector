from datetime import datetime, timedelta
import struct
import ipaddress
from array import array
import fxp_bytes
import socket

"""
Published mesages format: 
<timestamp, currency 1, currency 2, exchange rate>

Bytes[0:8] The timestamp is a 64-bit integer number of microseconds that have passed since 
00:00:00 UTC on 1 January 1970 (excluding leap seconds). Sent in big-endian network format.

Bytes[8:14] The currency names are the three-character ISO codes ('USD', 'GBP', 'EUR', etc.)
transmitted in 8-bit ASCII from left to right.

Bytes[14:22] The exchange rate is 64-bit floating point number represented in IEEE 754 binary64 little-endian format.
The rate is number of currency2 units to be exchanged per one unit of currency1. 
So, for example, if currency1 is USD and currency2 is JPY, we'd expect the exchange rate to be around 100.

Bytes[22:32] Reserved. These are not currently used (typically all set to 0-bits).

- You subscribe or renew subscriptions by sending listening IP address and port # to the provider. Subscriptions last for 10 mins.
Format of subscription request: 4-byte IPv4 in big-endian, followed by 2-byte port number in big-endian.
"""


MAX_QUOTES_PER_MESSAGE = 50
MICROS_PER_SECOND = 1_000_000

def deserialized_price(x: bytes) -> float:
    """
    Convert an array of byte to a float used in the price feed messages.
    The serialized is hex on a little-endian machine

    :param x: bytes received from a Forex Provider message
    return: price in float
    """

    [price] = struct.unpack('d', x)
    return price

def serialize_address(addr: tuple) -> bytes:
    """
    Serialize the host and port address we want the provider to publish to

    :param addr: the ip address and port pair
    :return: 6-byte sequence in subscription request
    """
    p = array('H', [addr[1]])
    p.byteswap()    #convert to big-endian
    ip = ipaddress.ip_address(addr[0])
    data = bytes()
    data += ip.packed + p.tobytes()
    return data


def deserialize_utcdatetime(stamp: bytes) -> datetime:
    """
    Convert a byte stream into a UTC datetime from a Forex Provider message, the data is an int of microseconds
    that have passed since 00:00:00 UTC on 1 January 1970.
    Bytes in big-endian network format.

    :param stamp: 8-byte stream of a an int to be deserialized
    :return: timestamp of the message
    """
    epoch = datetime(1970, 1, 1)
    time = int.from_bytes(stamp, 'big')

    sec = time / MICROS_PER_SECOND   #convert back to seconds
    timeStamp = epoch + timedelta(seconds=sec)
    return timeStamp

def unmarshal_message(msg: bytes) -> list:
    """
    Unmarshal a byte stream from Forex Provider with quotes
    >Each record is 32 bytes

    :param msg: byte stream of list of quotes
    :return the list of quotes as a list of dictionaries
    """
    QUOTE_LENGTH = 32
    endcoding = 'utf-8'
    timestamp = 8
    currency1 = timestamp + 3
    currency2 = currency1 + 3
    price = currency2 + 8
    
    #Reads the message for every 32 bytes
    quotes = (int) (len(msg) / QUOTE_LENGTH)

    toReturn = []
    start = startQ = 0
    stopQ = 33
    
    for quote in range(0, quotes):
        q = {}
        data = msg[startQ:stopQ]

        q['timestamp'] = deserialize_utcdatetime(data[start:start+timestamp])
        
        q['cross1']  = str(data[start+timestamp:start+currency1], endcoding)
        q['cross2']  = str(data[start+currency1:start+currency2], endcoding)
        
        q['price'] = deserialized_price(data[start+currency2:start+price])
        toReturn.append(q)

        startQ += 32
        stopQ += 32

    return toReturn

def subscribe(sock, host_addr: tuple, server_addr: tuple):
    """
    Format of subscription request: 4-byte IPv4 in big-endian, followed by 2-byte port number in big-endian.
    """
    
    # Create a UDP socket
    return sock.sendto(serialize_address(host_addr), server_addr)     #check in with the publisher