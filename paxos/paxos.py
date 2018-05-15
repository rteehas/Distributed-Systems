import sys


################### Proposer #################


class P:
    def __init__(self, n):
        self.n = n
        # used for printouts
        self.id = "P" + str(self.n)
        # what we use to store the counts
        self.counts = {
            'accepted': 0,
            'rejected': 0,
            'promised': 0
        }

        # store the top promise and top n
        self.top = {
            'top_n': None,
            'top_prom': None
        }

        # stores the values for whats accepted, what was previously received, and the proposal
        self.vals = {
            'prior_rec': None,
            'accepted': None,
            'proposal': None,
            'resend': None
        }

        # stores the boolean values used
        self.bools = {
            'promise': False,
            'propose': False
        }
        self.failed = False

    def return_to_init(self):
        """
        resets the proposer so that the values are the initial values (note, this doesn't include failure boolean
        which is dealt with in a different function)
        :return: 
        """
        for key in self.counts:
            self.counts[key] = 0

    def failure(self):
        """
        Puts the proposer in failure mode
        :return: 
        """

        self.failed = True

    def recovery(self):
        """
        puts the proposer in recovery mode, undoes failure
        :return: 
        """
        self.failed = False


################## Acceptor #################


class A:
    def __init__(self, n):
        self.n = n
        self.id = "A" + str(self.n)
        self.failed = False
        self.acc = None
        self.prev = None
        # initialize the highest responded number as negative so that anything positive is greater
        self.top_num = -10

    def failure(self):
        """
        puts acceptor in failure mode
        :return: 
        """
        self.failed = True

    def recovery(self):
        """
        puts acceptor in recovery mode
        :return: 
        """
        self.failed = False


#################### Message ##################


class Message:
    def __init__(self, n, v, type, source, dest, prev=None):
        self.n = n
        self.v = v
        self.type = type
        self.source = source
        self.dest = dest
        self.prev = prev

    # functions used to prepare the final strings depending on cases
    def prepare_pre(self):
        """
        Prepare the string for a prepare message
        :return: string
        """
        return "PREPARE n=" + str(self.n)

    def prepare_pro(self):
        """
        Prepares the string for a propose message 
        :return: string
        """
        return "PROPOSE v=" + str(self.v)

    def prepare_prom(self):
        """
        Prepares the string for a promise message
        :return: string
        """
        m = str()
        m += "PROMISE n=" + str(self.n)
        if self.prev is not None:
            m += " (Prior: n=" + str(self.prev[0]) + ", v=" + str(self.prev[1]) + ")"
        else:
            m += " (Prior: None)"

        return m

    def prepare_accept(self):
        """
        Prepares the string for an accept message
        :return: string
        """

        return "ACCEPT n=" + str(self.n) + " v=" + str(self.v)

    def prepare_accepted(self):
        """
        Prepares the string for an message of type accepted
        :return: string
        """
        return "ACCEPTED n=" + str(self.n) + " v=" + str(self.v)

    def prepare_rej(self):
        """
        Prepares the string for a rejection message
        :return: string
        """
        return "REJECTED n=" + str(self.n)

    def get_str(self):
        """
        Prints for the final printout
        :return: Returns the string we print out given the message type
        """
        final_str = str()

        if self.source != None:
            final_str += self.source.id
        else:
            final_str += "  "

        final_str += " -> " + self.dest.id + "  "

        # I used this resource as a model for a switch dictionary:
        # https://jaxenter.com/implement-switch-case-statement-python-138315.html
        switch_dict = {
            'accept':self.prepare_accept,
            'accepted': self.prepare_accepted,
            'rejected': self.prepare_rej,
            'propose': self.prepare_pro,
            'promise': self.prepare_prom,
            'prepare': self.prepare_pre
        }

        if self.type.lower() in switch_dict:
            final_str += switch_dict[self.type.lower()]()
        else:
            # for debugging
            print("ERROR")

        return final_str


########################## Queue #########################


class Queue:
    """
    Message queue
    """
    def __init__(self):
        self.queue = []

    def __len__(self):
        """
        Allows you to call len(queue)
        :return: int, length of the underlying list
        """
        return len(self.queue)

    def add(self, val):
        """
        Adds a value to the queue
        :param val: value, in this case messages
        :return: 
        """
        self.queue.append(val)

    def extract(self, do_nothing, t):
        """
        Extracts a message from the queue
        :param do_nothing: 
        :param t: 
        :return: 
        """
        for i in range(len(self.queue)):
            message = self.queue[i]
            dest = message.dest
            source = message.source
            if not dest.failed:
                if source is None:
                    del self.queue[i]
                    return message
                elif source.failed == False:

                    del self.queue[i]
                    return message
        else:
            if do_nothing:
                tick_str = "%03d: " % t
                print(tick_str)


######################### Simulator ######################


class Simulator:

    def __init__(self):
        self.n = 1
        self.props = []
        self.accs = []
        self.events = []
        self.message_list = Queue()
        self.max_tick = None

    def process_event(self, event, t):
        """
        Process an event given a tick number (used to print) and event
        :param event: tuple as specified in kattis
        :param t: tick number
        :return: 
        """
        if event is not None:
            # first we fail and recover the nodes, then we deal with the messages
            self.fail_nodes(event[1], t, True) # failure
            self.fail_nodes(event[2], t, False)  # recovery
            message = event[3]
            # only do_nothing if event is None
            do_nothing = False

            # send the message
            if message:
                self.send(message, t)

            # extract a message if the message isnt none
            elif message is None:
                m = self.message_list.extract(do_nothing, t)
                if m is not None:
                    self.send(m, t)

        else:
            # will be used to print an essentially blank line
            do_nothing = True
            message = self.message_list.extract(do_nothing, t)

            if message is not None:
                self.send(message, t)

    def handle_prepare(self, m, t):
        """
        Handles a message of type prepare
        :param m: message
        :param t: tick number
        :return: 
        """
        # only do something if its the latest to come
        if m.n > m.dest.top_num:
            new_message = Message(m.n, m.v, 'promise', m.dest, m.source, prev=m.dest.acc)
            self.message_list.add(new_message)
            m.dest.top_num = m.n

    def handle_promise(self, m, t):
        """
        Handle message of type promise
        :param m: message
        :param t: tick number
        :return: 
        """
        if m.dest.top['top_n'] is not None:
            if m.dest.top['top_n'] > m.n:
                return  # quit out

            elif m.dest.top['top_n'] < m.n:
                m.dest.counts['promised'] = 0

        if m.dest.bools['promise'] == True:
            return

        if m.prev is not None:
            m.dest.top['top_prom'] = m.prev

        # update the top n for the proposer
        m.dest.top['top_n'] = m.n

        # increment the count
        m.dest.counts['promised'] += 1

        # check if the majority has been met
        if (len(self.accs) / 2) < m.dest.counts['promised']:
            # change boolean values
            m.dest.vals['promised'] = True

            # update the values sent
            if m.dest.top['top_prom'] is not None:
                prom = m.dest.top['top_prom']
                new_v = prom[1]
            else:
                prop = m.dest.vals['proposal']
                new_v = prop[1]

            # send an accept message to every acceptor
            self.broadcast_acc(m.dest, m.dest.top['top_n'], new_v)
            m.dest.bools['promise'] = False
            m.dest.counts['promised'] = 0

    def send(self, m, t):
        """
        Sends a message
        :param m: message to be sent
        :param t: tick number
        :return: 
        """
        tick_str = "%03d:" % t
        print(tick_str + " " + m.get_str())

        if m.type.lower() == 'prepare':
            self.handle_prepare(m, t)

        elif m.type.lower() == 'propose':
            m.dest.bools['promise'] = False
            # extract the proposal
            m.dest.vals['proposal'] = (m.n, m.v)
            # send 'prepare' messages
            self.send_prepares(m.dest, m.n, m.v)

        elif m.type.lower() == 'accept':
            if m.n >= m.dest.top_num:
                m.dest.acc = (m.n, m.v)
                # reverse destination and source when sending this message
                self.message_list.add(Message(m.n, m.v, 'accepted', m.dest, m.source))
            else:
                self.message_list.add(Message(m.n, m.v, 'rejected', m.dest, m.source))

        elif m.type.lower() == 'accepted':
            m.dest.vals['accepted'] = m.v
            m.dest.counts['accepted'] += 1

        elif m.type.lower() == 'rejected':

            if m.dest.vals['resend'] != m.n:
                m.dest.counts['rejected'] = 1
                m.dest.vals['resend'] = m.n
            else:
                m.dest.counts['rejected'] += 1
            if (len(self.accs) / 2) < m.dest.counts['rejected']:
                new_v = None
                self.send_prepares(m.dest, self.n, new_v)
                self.n += 1

        else:
            # last one is promise
            self.handle_promise(m, t)

    def add_event(self, t, p, val, F, R):
        """
        Creates and adds an event to the event queue
        :param t: last tick
        :param p: proposer
        :param val: value
        :param F: failure set
        :param R: recover let
        :return: 
        """
        if t is not None:
            if val is not None and p is not None:
                event = (t, F, R, Message(self.n, val, 'propose', None, p))
                self.n += 1
            else:
                event = (t, F, R, None)
        else:
            return

        self.events.append(event)

    def send_prepares(self, p, n, v):
        """
        Sends prepare messages, used after propose
        :param p: Proposor that is sending the message
        :param n: int, n value
        :param v: int, v value
        :return: 
        """
        p.return_to_init()
        # send to every acceptor
        for a in self.accs:
            self.message_list.add(Message(n, v, 'prepare', p, a))

    def broadcast_acc(self, p, n, v):
        """
        Broadcast an accept message to all acceptors
        :param p: Proposor sending the message
        :param n: int, n value
        :param v: int, v value
        :return: 
        """
        for a in self.accs:
            self.message_list.add(Message(n, v, 'accept', p, a))

    def fail_nodes(self, nodes, t, fail):
        """
        Decides if nodes should be failed and prints to stdout
        :param: nodes: list of nodes to fail or recover
        :param: t: tick number
        :param: fail: boolean that tells you whether to fail or recover
        :return: 
        """
        s = str()
        tick_str = "%03d" % t
        for node in nodes:
            if fail:
                node.failure()
                s += tick_str + ":" + " ** " + node.id + " " + "FAILS **\n"
            else:
                node.recovery()
                s += tick_str + ":" + " ** " + node.id + " " + "RECOVERS **\n"

        s = s.rstrip('\n') # get rid of the last \n
        if len(s) > 0:
            print(s)

    def get_results(self):
        """
        Prints the final results from a proposer
        :param p: proposer
        :return:
        """
        for p in self.props:
            num_acc = len(self.accs)
            final = p.id
            if p.counts['accepted'] > num_acc / 2:
                final += " has reached consensus (proposed " + str(p.vals['proposal'][1]) + ", accepted "
                final += str(p.vals['accepted']) + ")"
            else:
                final += " did not reach consensus"
            print('\n' + final)

    def simulate(self):
        """
        Runs the simulation
        :return: 
        """
        for i in range(self.max_tick + 1):
            if len(self.events) == 0:
                if len(self.message_list) == 0:
                    # if events and message lists are empty, end
                    break
                else:
                    # if events are empty but there are messages, add None
                    new_event = None
            else:
                if self.events[0][0] == i:
                    # if the event is happening at this time, pass the event through
                    new_event = self.events[0]
                    self.events = self.events[1:]
                else:
                    # otherwise, add None
                    new_event = None

            # process the event
            self.process_event(new_event, i)

        # print the results
        self.get_results()

if __name__ == "__main__":
    # initial values
    t = None
    v = None

    # failure and recovery
    fails = None
    reco = None

    # initial proposer is none
    prop = None
    sim = Simulator()

    # first fill the lines
    read = True
    lines = []
    while read:
        line = sys.stdin.readline()
        lines.append(line)
        if line == '0 END\n':
            read = False

    # read the events
    for i in range(len(lines)):

        line = lines[i].strip('\n').split(" ")
        if i == 0:
            # fill the simulator in the first line
            n_props = int(line[0])
            n_acc = int(line[1])
            for j in range(1, n_acc + 1):
                sim.accs.append(A(j))
            for j in range(1, n_props + 1):
                sim.props.append(P(j))
            sim.max_tick = int(line[2])

        else:
            # deal with the last line
            if line[0] == '0' and line[1] == 'END':
                sim.add_event(t, prop, v, fails, reco)

            else:
                new_t = int(line[0])
                if new_t != t:
                    if t is not None:
                        # add an event
                        sim.add_event(t, prop, v, fails, reco)
                        prop = None
                        v = None

                    # reset the values after you add an event
                    fails = []
                    reco = []
                    t = new_t

                # deal with the specific event cases
                if line[1] == 'PROPOSE':
                    prop = sim.props[int(line[2]) - 1]
                    v = int(line[3])

                elif line[1] == 'RECOVER':
                    if line[2] == 'PROPOSER':
                        node = sim.props[int(line[3]) - 1]

                    elif line[2] == 'ACCEPTOR':
                        node = sim.accs[int(line[3]) - 1]
                    reco.append(node)

                elif line[1] == 'FAIL':
                    if line[2] == 'PROPOSER':
                        node = sim.props[int(line[3]) - 1]

                    elif line[2] == 'ACCEPTOR':
                        node = sim.accs[int(line[3]) - 1]

                    fails.append(node)

    sim.simulate()
