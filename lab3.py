import socket
from fxp_bytes_subscriber import unmarshal_message, subscribe
from bellman_ford import BellmanFord

"""
This program that tracks and report on arbitrage opportunies by providng the follow:

1. subscribe to the forex publishing service,
2. for each message published, update a graph based on the published prices,
3. run Bellman-Ford, and
4. report any arbitrage opportunities.

> A published price is assumed to remain in force for 1.5 seconds or,
until a superseding rate is observed. 

> Quotes can be out of order (because of UDP), hence process should ignore any quotes with timestamps
before the lastest one seen for that market.
> Graph is weighted by negative log.
> Each message from provider may have multiple quotes in it:
    > Each would superses any prior quote for a market,
    > Excluded markets will use the most recent quote, as long as it's not older than 1.5 seconds.
    > Each quote will have: 2 nodes in c1 -> c2 direction: (c1, c2, -log(rate))
        > 1 edge going c2 -> c1 direction: (c2, c1, log(rate))

> If there's a negative cycle, report by listing all trades included on that cycle. Whenever there are multiple
negative cycles, just need to report whichever was discovered first.

> Step 2 - 4 are repeated for 10 minutes until the algorithm needs to renew subscription or we shut down.
    > Can also shutdown if not receive any published message for > 1 minute.
"""

#TODO: move the subscribe loop back here, only send subscribe message from subscriber 

class Lab3:

    SUB_EXPIRATION = 600    #10 minutes in seconds
    VALID_DURATION = 1.5    #1.5 seconds

    def __init__(self, addr, prov) -> None:
        self.listener_address = addr
        self.provider_address = prov

    def run(self):
        g = BellmanFord({'a': {'b': 1, 'c':5}, 'b': {'c': 2, 'a': 10}, 'c': {'a': 14, 'd': -3}, 'e': {'a': 100}})
        g.shortest_paths('a')
        g.add_edge(('a', 'e', -200))
        g.shortest_paths('a')

        exit(1)

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(self.listener_address)  # subscriber binds the socket to the publishers address
            sent = subscribe(sock, self.listener_address, self.provider_address)
            
            while True:
                print('\nblocking, waiting to receive message')
                data = sock.recv(4096)

                print('received {} bytes'.format(len(data)))
                self.prt_quote(unmarshal_message(data))


    def prt_quote(self, data: list):
        """
        Helper to print the quotes out with a format
        """

        for quote in data:
            time = quote['timestamp'].strftime('%Y/%m/%d %H:%M:%S.%f')
            curr = quote['cross1']
            curr += ' ' + quote['cross2']
            price = quote['price']

            print('{} {} {}'.format(time, curr, price))

if __name__ == '__main__':
    """     
    PRICE = 9006104071832581.0
    ser_price = fxp_bytes.serialize_price(PRICE)
    print('price serialized: {}'.format(ser_price))
    
    print('price deserialized: {}'.format(deserialized_price(ser_price)))

    time = datetime.now() 
    ser_time = fxp_bytes.serialize_utcdatetime(time)

    print('Time before formatting: {}, after: {}'.format(time, ser_time))
    deserialize_utcdatetime(ser_time) 
    
    
    b = fxp_bytes.marshal_message([{'timestamp': datetime(2006,1,2), 'cross': 'GBP/USD', 'price': 1.22041},
                             {'timestamp': datetime(2006,1,1), 'cross': 'USD/JPY', 'price': 108.2755}])
    print(unmarshal_message(b))

    print(fxp_bytes.deserialize_address(serialize_address(('127.0.0.1', 65534))))"""

    ADDRESS = ('127.0.0.1', 22222)
    SERVER_ADDRESS = ('localhost', 50403)

    lab3 = Lab3(ADDRESS, SERVER_ADDRESS)
    lab3.run()