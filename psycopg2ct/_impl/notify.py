

class Notify(object):
    def __init__(self, pid, channel, payload=''):
        self.pid = pid
        self.channel = channel
        self.payload = payload

    def __eq__(self, other):
        if isinstance(other, tuple):
            return other == self._astuple(False)
        if isinstance(other, Notify):
            return self._astuple(True) == other._astuple(True)
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._astuple(bool(self.payload)))

    def __getitem__(self, key):
        return (self.pid, self.channel)[key]

    def __len__(self):
        return 2

    def _astuple(self, with_payload):
        if not with_payload:
            return (self.pid, self.channel)
        return (self.pid, self.channel, self.payload)
