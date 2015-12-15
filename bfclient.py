import sys, json, socket, time
from threading import Thread
from copy import deepcopy

MAG = 15

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
        self.origin = weight
        self.link_status = True
        self.name = ':'.join((self._addr, str(self._port)))
        self.time = time.time()

    def get_addr(self):
        return self._addr

    def get_port(self):
        return self._port


class DV():
    def __init__(self, addr='127.0.0.1', port=4118, timeout=3.0, *args):
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
        print args
        # controlling flags
        # log the update time.
        self.timer_due = False
        # send the distance vector as long as it's initialized.
        self.dv_update_flag = True
        self.ceiling = 0 # 10 * max weight, because there are max of ten nodes in our simulation
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
                    if k != name:
                        self.distance_vector[name][k] = float('inf')
                    else:
                        self.distance_vector[name][name] = 0

    def is_neighbor_alive(self):
        for k,v in self.neighbors.items():
            if k!= self.host_name and v.link_status:
                return True
        return False

    def show_rt(self):
        if self.is_neighbor_alive():
            dv = self.distance_vector[self.host_name]
            for k, v in dv.items():
                if v != float('inf') and k != self.host_name:
                    print 'Destination={0}, Cost={1}, (next hop) link={2}'.format(k,v,self.next_hop[k])
        else:
            print 'No NEIGHBOR IS ALIVE.'

    def close(self):
        # close the timer first.
        self.timer_due = False
        self.dv_update_flag = False
        print('Closing...')
        self.conn.close()
        sys.exit('Closed.')

    def print_table(self):
        print 'dv_table'.center(80, '-')
        for name in self.distance_vector.keys():
            print self.distance_vector[name]

        print self.next_hop
        print ''.center(80, '-') + '\n'

    # this is the core program, which will compute the distance table.
    def bellman_ford(self):
        # avoid re-write the dv because the update_dv will modify the dv when it receives.
        distance_vector = deepcopy(self.distance_vector)
        next_hopper = deepcopy(self.next_hop)
        update = False
        for dest, cost in distance_vector[self.host_name].items():
            min = float('inf')
            next_hop = None
            if dest != self.host_name:
                for mid,v in self.neighbors.items():
                    if v.link_status and mid !=self.host_name:
                        # here is a fatal error that I use the dv ttable to modify the dv table.
                        d = self.neighbors[mid].weight + distance_vector[mid][dest]
                        # including d == inf.
                        if d <= min:
                            min = d
                            next_hop = mid

                # RESET THE TABLE HERE IF THE MIN IS THE DIRECT EDGE BUT THE VALUE DOSEN'T MATCH
                # if dest in self.neighbors.keys() and next_hop == dest and min != self.neighbors[dest].weight \
                #         and (distance_vector[next_hopper[dest]][dest] == float('inf') or
                #         distance_vector[dest][next_hopper[dest]] == float('inf')):
                #     min = self.neighbors[dest].weight
                #     distance_vector[self.host_name][dest] = self.neighbors[dest].weight
                #     next_hop = dest

                if min != cost:
                    if min != float('inf') and min > self.ceiling:
                        min = float('inf')
                    distance_vector[self.host_name][dest] = min
                    next_hopper[dest] = next_hop
                    update = True

        self.distance_vector = distance_vector
        self.next_hop = next_hopper
        # self.print_table()
        # print 'update: {}, timer: {}'.format(update, self.timer_due)
        return update

    # used for receiving the msg from the neighbors.
    # update the timer of the near neighbor when it receives the msg from neighbor.
    def update_dv(self, msg, addr, link=True):
        # addr is tuple of (host, port)
        # the client must be the neighbor
        client = ':'.join([addr[0], str(addr[1])])
        # update the dv table.
        if link:
            # if recv the UPDATE info, the status will be restored.
            if not self.neighbors[client].link_status:
                self.neighbors[client].link_status = True
                self.neighbors[client].weight = self.neighbors[client].origin
            # update the time.
            self.neighbors[client].time = time.time()
            # print '>>> timer update: ' + str(addr)
            # self.timer = time.time()
            try:
                tmp = json.loads(msg)
            except Exception, e:
                print str(e) + 'json loads exception'
            else:
                # once I find uew node in the entry, the dv table will be expanded.
                diff = set(tmp.keys())-set(self.distance_vector[client].keys())
                # print '>>>>diff {}'.format(diff)
                if diff:
                    new_node = list(diff)
                    for name in new_node:
                        self.next_hop[name] = name
                        for neighbor in self.neighbors.keys():
                            if neighbor != client:
                                self.distance_vector[neighbor][name] = float('inf')
                    # update the client entry.
                    for k,v in tmp.items():
                        self.distance_vector[client][k] = v
                else:
                    # the node will set inf when it can't be reachable.
                    for k,v in tmp.items():
                        self.distance_vector[client][k] = v
        else:
            self.neighbors[client].link_status = False
            self.neighbors[client].weight = float('inf')
            # update the table here

        # Bellman-Ford Algorithm.
        self.dv_update_flag = self.bellman_ford()


    def recv(self):
        while True:
            msg, addr = self.conn.recvfrom(1024)
            if msg.startswith('ROUTE UPDATE'):
                # print str(addr) + msg
                self.update_dv(msg[len('ROUTE UPDATE'):], addr)
            elif msg.startswith('LINK DOWN'):
                self.update_dv('',addr,False)
            elif msg.startswith('LINK UP'):
                self.update_dv(msg[len('LINK UP'):],addr)
            else:
                print'error >>> '+ msg

    def send(self):
        # wrap the msg into the json format when there is timeout or update of the table.
        while True:
            if self.dv_update_flag or self.timer_due:
                # set the sending timer.
                self.timer = time.time()
                msg = 'ROUTE UPDATE'
                msg += json.dumps(self.distance_vector[self.host_name],separators=(',', ': '))
                # print msg
                for k,v in self.neighbors.items():
                    # send to all the node that still links.
                    if k != self.host_name and v.link_status:
                        addr = k.split(':')
                        # print 'sent to' + str(addr)
                        self.conn.sendto(msg, (addr[0], int(addr[1])))
                if self.timer_due:
                    self.timer_due = False

                if self.dv_update_flag:
                    self.dv_update_flag = False

    # reset the table if one of neighbor is down.
    def reset_dv_host(self, name):
        for k, v in self.neighbors.items():
            if k != name and v.link_status:
                self.distance_vector[self.host_name][k] = self.neighbors[k].weight
                # THIS IS VERY IMPORTANT DEBUG!!!!! YOU SHOULD RESET NEXT HOP HERE
                # BECAUSE IN YOU BELLMAN FORD, YOU USE != TO DECIDE THE NEXT HOPPER
                # SO IF YOU RESET THE FIRST ENTRY AND THIS ITEM ITSELF IS THE MIN, IT WON'T
                # WRITE THE NEXT HOPPER INTO THE LIST.
                self.next_hop[k] = k

    def timer_(self):
        while True:
            current = time.time()
            if (current - self.timer) > self.timeout:
                if not self.dv_update_flag and self.is_neighbor_alive():
                    self.timer_due = True
                self.timer = current

            for name in self.neighbors:
                if self.neighbors[name].link_status and name != self.host_name:
                    if (current - self.neighbors[name].time) > 3 * self.timeout:
                        # print '>>> timer '+ name + ': ' + str(current - self.neighbors[name].time)
                        self.neighbors[name].link_status = False
                        self.neighbors[name].weight = float('inf')
                        # self.reset_dv_host(name)
                        self.dv_update_flag = self.bellman_ford()

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
                if not self.neighbors[client].link_status:
                        print 'LINK DOWN ALREADY.'
                        return
                msg = 'LINK DOWN'
                try:
                    self.conn.sendto(msg, (addr[0], int(addr[1])))
                except ValueError, e:
                    print e
                    sys.exit('link_down value error')
                except socket.error, e:
                    print 'ERROR{0}: {1}'.format(e[0], e[1])
                    sys.exit('link_down socket error')
                print '>>> LINK DOWN ' + client
                self.neighbors[client].link_status = False
                self.neighbors[client].weight = float('inf')
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
            if client == self.host_name:
                print 'CANT LINK UP YOURSELF'
                return
            if client in self.neighbors.keys():
                try:
                    if self.neighbors[client].link_status:
                        print 'LINK UP ALREADY.'
                        return
                    else:
                        print '>>> LINK UP ' + client
                        cl = self.neighbors[client]
                        # reset the link status.
                        cl.link_status = True
                        cl.weight = cl.origin
                        # reset client to it self.
                        self.next_hop[client] = client
                        for k in self.distance_vector[client].keys():
                            if k != client:
                                self.distance_vector[client][k] = float('inf')
                            else:
                                self.distance_vector[client][k] = 0
                        self.dv_update_flag = self.bellman_ford()
                        try:
                            self.conn.sendto('LINK UP' + json.dumps(self.distance_vector[self.host_name]),
                                             (addr[0], int(addr[1])))
                        except ValueError, e:
                            print e
                            sys.exit('link up exception.')
                except KeyError, e:
                    print e
                    sys.exit('ERROR LINK UP')
            else:
                print 'NO SUCH NEIGHBOR, INPUT AGAIN.'

    def df_client(self):
        if len(self.args) % 3 != 0:
            print 'ERROR: neighbor input syntax error'
            sys.exit(4)
        # init the neighborhood
        try:
            for i in range(0, len(self.args), 3):
                self.init_neighbor(self.args[i], int(self.args[i+1]), float(self.args[i+2]))
        except Exception, e:
            print(e)
            sys.exit(0)

        # init the distance vector table.
        self.init_dv_table()

        self.ceiling = MAG * max([v.weight for v in self.neighbors.values()])
        print 'The ceiling is: ' + str(self.ceiling)
        # start the daemon thread
        recv = Thread(target=self.recv)
        recv.daemon = True
        recv.start()
        send = Thread(target=self.send)
        send.daemon = True
        send.start()
        timer = Thread(target=self.timer_)
        timer.daemon = True
        timer.start()
        # three threads here.
        while True:
            cmd = raw_input(self.host_name+' >>>')
            cmd = cmd.strip().split(' ', 1) # divide the order into two.
            length = len(cmd)
            if cmd[0] == 'SHOWRT' and length == 1:
                self.show_rt()
            elif cmd[0] == 'CLOSE' and length == 1:
                self.close()
            elif cmd[0] == 'LINKUP' and length == 2:
                self.link_up(cmd[1])
            elif cmd[0] == 'LINKDOWN' and length == 2:
                self.link_down(cmd[1])
            elif cmd[0] == 'FG' and length == 1:
                print 'update: {}, timer: {}'.format(self.dv_update_flag, self.timer_due)
            elif cmd[0] == '':
                continue
            elif cmd[0].startswith('TA'):
                self.print_table()
            else:
                print 'cmd error, pls input again.'


if __name__ == '__main__':
    try:
        dv = DV(sys.argv[1],int(sys.argv[2]), float(sys.argv[3]), *sys.argv[4:])
    except ValueError, e:
        print e
        sys.exit(0)
    else:
        try:
            dv.df_client()
        except KeyboardInterrupt, e:
            print e
            sys.exit('Terminated by user.')

