import sys, json, socket, time
from threading import Thread


class Neighbor():
    def __init__(self, addr='127.0.0.1', port=4119, weight=5.0):
        self._addr = addr
        self._port = port
        # topological info
        try:
            weight = float(weight)
            if weight < 0:
                raise ValueError('weight less than 0')
        except ValueError, e:
            print str(e) + ' invalid weight.'
            sys.exit(3)
        self.weight = weight
        self.link_status = True
        self.name = ':'.join((self._addr, str(self._port)))
        self.time = time.time()

    def get_addr(self):
        return self._addr

    def get_port(self):
        return self._port


class DV():
    def __init__(self, addr='127.0.0.1', port=4118, timeout=3, *args):
        self.addr = addr
        self.port = port
        self.host_name = ':'.join((self.addr,str(self.port)))
        self.timer = time.time()
        self.neighbors = dict()
        self.next_hop = dict()
        # set the timeout time
        self.timeout = timeout
        self.neighbors.setdefault(self.host_name, Neighbor(self.addr, self.port, 0))
        self.distance_vector = dict()
        self.args = args
        # controlling flags
        # log the update time.
        self.timer_due = False
        # send the distance vector as long as it's initialized.
        self.dv_update_flag = True
        # the neighbor that updates its dv.
        try:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.conn.bind((self.addr,self.port))
        except socket.error, e:
            print 'socket error[{0}]: {1}'.format(e[0], e[1])
            sys.exit(5)

    # the neighbor info
    def init_neighbor(self, addr='127.0.0.1', port=4117, weight=6.0):
        neighbor = Neighbor(addr, port, weight)
        client = ':'.join((addr,str(port)))
        self.neighbors[client] = neighbor
        self.next_hop[client] = client


    # init the dv table
    def init_dv_table(self):
        neighborhood = list(self.neighbors.keys())
        for name in neighborhood:
            self.distance_vector[name] = dict()
            if name == self.host_name:
                for k,v in self.neighbors.items():
                    self.distance_vector[name][k] = v.weight
                self.distance_vector[name][name] = 0
            else:
                for k in neighborhood:
                    self.distance_vector[name][k] = float('inf')

    def is_neighbor_alive(self):
        for k,v in self.neighbors.items():
            if k!= self.host_name and v.link_status:
                return True
        return False

    def show_rt(self):
        if self.is_neighbor_alive():
            dv = self.distance_vector[self.host_name]
            for k,v in dv.items():
                if k != self.host_name and self.neighbors[k].link_status:
                    if v != float('inf'):
                        print 'Destination={0}, Cost={1}, (next hop) link={2}'.format(k,v,self.next_hop[k])
        else:
            return 'No neighbor is alive.'

    def close(self):
        for k,v in self.neighbors:
            if k != self.host_name:
                addr, port = k.split(':')
                self.conn.sendto('CLOSE', (addr, port))
        # close the timer first.
        self.timer_due = False
        print('Closing...')
        self.conn.close()
        sys.exit('Closed.')

    def bellman_ford(self):
        update = False
        for dest, cost in self.distance_vector[self.host_name].items():
            min = float('inf')
            next_hop = None
            # init the first distance.
            d = cost
            if dest != self.host_name:
                for mid,v in self.neighbors.items():
                    if v.link_status and mid !=self.host_name:
                        d = self.distance_vector[self.host_name][mid]+self.distance_vector[mid][dest]
                        # including d == inf.
                        if d <= min:
                            min = d
                            next_hop = mid
                if d != cost:
                    self.distance_vector[self.host_name][dest] = d
                    self.next_hop[dest] = next_hop
                    update = True
        return update

    # used for receiving the msg from the neighbors.
    def update_dv(self, msg, addr, link=True):
        # addr is tuple of (host, port)
        client = ':'.join([addr[0], str(addr[1])])
        # update the dv table.
        if link:
            # if recv the UPDATE info, the status will be restored.
            self.neighbors[client].link_status = True
            # update the time.
            self.neighbors[client].time = time.time()
            self.timer = time.time()
            try:
                tmp = json.loads(msg)
            except Exception, e:
                print str(e) + 'json loads exception'
            else:
                # once I find uew node in the entry, the dv table will be expanded.
                diff = set(tmp.keys())-set(self.distance_vector[client].keys())
                if diff:
                    new_node = list(diff)
                    for name in new_node:
                        for neighbor in self.neighbors.keys():
                            if neighbor != client:
                                self.distance_vector[neighbor][name] = float('inf')
                    # update the client entry.
                    for k,v in tmp:
                        self.distance_vector[client].setdfault(k,v)
                else:
                    # the node will set inf when it can't be reachable.
                    for k,v in tmp.items():
                        self.distance_vector[client][k] = v
        else:
            self.neighbors[client].link_status = False
            self.distance_vector[self.host_name][client] = float('inf')
            # update the table here
            
        # Bellman-Ford Algorithm.
        self.dv_update_flag = self.bellman_ford()


    def recv(self):
        while True:
            msg, addr = self.conn.recvfrom(1024)
            if msg.startswith('ROUTE UPDATE'):
                self.update_dv(msg[len('ROUTE UPDATE'):], addr)
            elif msg.startswith('CLOSE') or msg.startswith('LINK DOWN'):
                self.update_dv('',addr,False)
            elif msg.startswith('LINK UP'):
                self.update_dv(msg[len('LINK UP'):],addr)
            else:
                print'error >>> '+ msg

    def send(self):
        while True:
            if self.dv_update_flag or self.timer_due:
                self.timer = time.time()
                msg = 'ROUTE UPDATE'
                msg += json.dumps(self.distance_vector[self.host_name],separators=(',', ': '))
                for k,v in self.neighbors.items():
                    # send to all the node that still links.
                    if k != self.host_name and v.link_status:
                        self.conn.sendto(msg, k.split(':'))
                if self.timer_due:
                    self.timer_due = False
                    continue
                if self.dv_update_flag:
                    self.dv_update = False

    def timer(self):
        while True:
            current = time.time()
            if (current - self.timer) > self.timeout:
                if not self.dv_update_flag:
                    self.timer_due = True
                self.timer = current
            for name in self.neighbors:
                if self.neighbors[name].link_status:
                    if (current - self.neighbors[name].time) > 3 * self.timeout:
                        self.neighbors[name].link_staus = False

    def link_down(self, cmd):
        # for str input
        if type(cmd) == str:
            addr = cmd.split()
        else:
            # for the iterable like tuple and list
            addr = cmd
        if len(addr) != 2:
            print 'cmd format error'
        else:
            client = ':'.join(addr)
            if client == self.host_name:
                print 'CANT LINK DOWN YOURSELF'
                return
            if client in self.neighbors.keys():
                msg = 'LINK DOWN'
                try:
                    self.conn.sendto(msg, (addr[0], int(addr[1])))
                except ValueError, e:
                    print e
                    sys.exit('link_down value error')
                except socket.error, e:
                    print 'ERROR{0}: {1}'.format(e[0], e[1])
                    sys.exit('link_down socket error')
                self.neighbors[client].link_status = False
                self.distance_vector[self.host_name][client] = float('inf')
                # update and send out the distance vector
                self.dv_update_flag = self.bellman_ford()
            else:
                print 'NO SUCH NEIGHBOR, INPUT AGAIN.'


    def link_up(self, cmd):
        # restart the timer
        self.timer = time.time()
        if type(cmd) == str:
            addr = cmd.split()
        else:
            # for the iterable like tuple and list
            addr = cmd
        if len(addr) != 2:
            print 'cmd format error'
        else:
            client = ':'.join(addr)
            try:
                if self.neighbors[client].link_status:
                    print 'LINK ALREADY.'
                    return
                else:
                    cl = self.neighbors[client]
                    # reset the link status.
                    cl.link_status = True
                    self.distance_vector[self.host_name][client] = cl.weight
                    self.dv_update_flag = self.bellman_ford()
                    try:
                        self.conn.sendto('LINK UP', (addr[0], int(addr[1])))
                    except ValueError, e:
                        print e
                        sys.exit('link up exception.')
            except KeyError, e:
                print e
                sys.exit('ERROR LINK UP')

    def df_client(self):
        if len(self.args) % 3 == 0:
            print 'ERROR: neighbor input syntax error'
            sys.exit(4)
        # init the neighborhood
        try:
            for i in range(0, len(self.args), 3):
                self.init_neighbor(self.args[i], self.args[i+1], self.args[i+2])
        except Exception, e:
            print(e)
            sys.exit(0)

        # init the distance vector table.
        self.init_dv_table()
        # start the daemon thread
        recv = Thread(target=self.recv)
        recv.daemon = True
        recv.start()
        send = Thread(target=self.send)
        send.daemon = True
        send.start()
        timer = Thread(target=self.timer)
        timer.daemon = True
        timer.start()
        # three threads here.
        while True:
            cmd = raw_input(self.host_name+' >>>').strip().split(' ', 1) # divide the order into two.
            length = len(cmd)
            if cmd[0] == 'SHOUWRT' and length == 1:
                self.show_rt()
            elif cmd[0] == 'CLOSE' and length == 1:
                self.close()
            elif cmd[0] == 'LINKUP' and length == 2:
                self.link_up(cmd[1])
            elif cmd[0] == 'LINKDOWN' and length == 2:
                self.link_down(cmd[1])
            else:
                print 'cmd error, pls input again.'


if __name__ == '__main__':
    dv = DV()
    dv.init_neighbor()
    dv.init_neighbor(port=4115, weight=3.0)
    dv.init_dv_table()
    dv.show_rt()
