from logging import raiseExceptions
import socket
from fxp_bytes_subscriber import unmarshal_message, subscribe
from bellman_ford import BellmanFord
from math import log
from datetime import datetime
import selectors

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

    SUB_EXPIRATION = 30    #10 minutes in seconds
    VALID_DURATION = 1.5    #1.5 seconds
    SELECTOR_CHECK = 0.3
    BUF_SZ = 4096

    def __init__(self, addr, prov) -> None:
        self.provider_address = prov
        self.marketLibrary = {}

        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

        self.foundLoop = False

    def start_a_server(self):
        """Function to start a generic server
        
        :return: a tuple of the socket that's the server, and the socket's name"""
        node_address = ('localhost', 0)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(node_address)
        sock.setblocking(False)
        return (sock, sock.getsockname())

    def run(self):
        self.g = BellmanFord()

        #Registering the socket without a callback for this socket
        self.selector.register(self.listener, selectors.EVENT_READ, data=None)

        #with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        #sock.bind(self.listener_address)  # subscriber binds the socket to the publishers address
        sent = subscribe(self.listener, self.listener_address, self.provider_address)
        timer = self.stamp()

        while True:
            events = self.selector.select(self.SELECTOR_CHECK)
            for key, mask in events:
                #When a peer sent me a message
                if mask & selectors.EVENT_READ:
                    self.receive_msg(key.fileobj)
            
            self.check_timeouts(timer)

            #data = self.listener.recv(4096)


    def receive_msg(self,sock):
        try:
            data = sock.recv(self.BUF_SZ)
            #print(msg)
        except Exception as err:
            print('Failure accepting data from peer: {}'.format(err))
        else:
            self.checkArbitrage(data)

    def check_timeouts(self, timer):
        if self.is_expired(timer, self.SUB_EXPIRATION):
            print('Subscription expired. Shutting down...')
            exit(1)

        #TODO: do I need to do this asynchronously?
        self.checkExpiredQuotes()


    def checkExpiredQuotes(self):
        """
        Function to remove expired quotes from our library and our graph

        :param: g: the graph containing the edges
        """

        #print('checking expired quotes')
        time = 0
        for market in self.marketLibrary:
            #Remove quote when expired
            if self.is_expired(self.marketLibrary[market][time], self.VALID_DURATION):
                print('removing stale quote for ({}, {})'.format(market[0], market[1]))
                self.g.remove_edge(market[0, market[1]])
                del self.marketLibrary[market]


    def checkArbitrage(self, data:dict):
        """
        Function to process the quotes and check for arbitrage opportunities

        :param: g: the graph used to track arbitrage
        :param: data: message received from provider
        """
        #Unload the messages
        quotes = unmarshal_message(data)

        for quote in quotes:
            self.prt_quote(quote)

            #add check for time, store seen quotes in a timer tracker
            self.add_market(quote)

            #only accept in order messages
            if not self.is_in_order(quote): 
                print('\nIgnoring out-of-order message')
                continue

            #Otherwise, store it in our Bellman algorithm
            edge = (quote['cross1'], quote['cross2'], -1 * log(quote['price']))
            recipEdge = (quote['cross2'], quote['cross1'], log(quote['price']))
            self.g.add_edge(edge)
            self.g.add_edge(recipEdge)

            dist, pred, neg_cycle = self.g.shortest_paths('USD', tolerance=-0)
            if neg_cycle:
                #print('arbitrage', neg_cycle, pred, dist)
                self.print_path(self.get_path(pred, neg_cycle))
                #exit(1)

                if self.foundLoop:
                    print(self.g.edges)
                    #exit(1)

    def get_path(self, pred, cycle):
        vertex = cycle[0]
        path = list()
        self.get_path_recur(pred, vertex, path)
        path.append(cycle[1])

        print(path)

        return path

    def get_path_recur(self, pred, vertex, path, counter = 0):
        """
        Helper function to recursively look for the predecessor of a vertice in the shortest path

        :param: pred: the predecessor dictionary
        :param: vertex: the vertex whose parent we need
        :param: path: a memory to conveniently store the parents in order
        """
        if counter == 15:
            print('Inifinity loop.')
            self.foundLoop = True
            return

        #base condition
        if pred[vertex] is None:
            path.append(vertex)
            return
        
        self.get_path_recur(pred, pred[vertex], path, counter + 1)
        path.append(vertex)

    def print_path(self, path):
        PRICE = 1
        units = 100

        i = 0
        print('ARBITRAGE:\n\tstart with USD ', units)
        while(i < len(path)- 1):
            try:
                market = (path[i], path[i+1])
                rate = self.marketLibrary[market][PRICE]
                units = units * rate
                print('\n\texchange {} for {} at {} -----> {} {}'.format(market[0], market[1], rate, market[1], units))

            #case when the direction is backwards
            except KeyError as e:
                market = (path[i+1], path[i])
                rate = 1 / self.marketLibrary[market][PRICE]
                units = units * rate
                print('\n\texchange {} for {} at {} -----> {} {}'.format(market[1], market[0], rate, market[0], units))
            
            i+=1


    def add_market(self, quote):
        key = (quote['cross1'], quote['cross2'])
        time = quote['timestamp']
        price = quote['price']

        #start tracking if not in library already
        if key not in self.marketLibrary:
            self.marketLibrary[key] = [time, price]
        
    def is_in_order(self, quote):
        TIME_KEY = 0
        PRICE_KEY = 1
        key = (quote['cross1'], quote['cross2'])
        time = quote['timestamp']
        price = quote['price']

        #if already in library, make sure the quote is in order
        if time < self.marketLibrary[key][TIME_KEY]:
            return False

        #otherwise update
        print('Updating timestamp')

        self.marketLibrary[key][TIME_KEY] = time
        self.marketLibrary[key][PRICE_KEY] = price
        return True

    def is_expired(self, stamp, threshold):
        """Helper function to check if the time has passed the threshold
        
        :param: peer: the peer to get time checked
        :param: threshold: the threshold to check against
        :return: True if timepassed is greater than threshold. False otherwise"""

        timepassed = (datetime.now() - stamp).total_seconds()    #get the time delta in seconds
        return timepassed > threshold

    @staticmethod
    def stamp():
        """Static helper function to give the current time
        
        :return: current time as a Datetime object"""
        return datetime.now() 

    def prt_quotes(self, data: list):
        """
        Helper to print the quotes out with a format
        """

        for quote in data:
            self.prt_quote(quote)
            

    def prt_quote(self, quote):
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