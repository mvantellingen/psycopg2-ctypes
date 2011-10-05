
class Xid(object):
    def __init__(self, format_id, gtrid, bqual):
        if not 0 <= format_id <= 0x7FFFFFFF:
            raise ValueError("format_id must be a non-negative 32-bit integer")

        if len(gtrid) > 64:
            raise ValueError("gtrid must be a string no longer than 64 characters")

        for char in gtrid:
            if not 0x20 <= ord(char) <= 0x7F:
                raise ValueError("gtrid must contain only printable characters")

        if len(bqual) > 64:
            raise ValueError("bqual must be a string no longer than 64 characters")

        for char in bqual:
            if not 0x20 <= ord(char) <= 0x7F:
                raise ValueError("bqual must contain only printable characters")

        self.format_id = format_id
        self.gtrid = gtrid
        self.bqual = bqual

    def as_tid(self):
        gtrid = self.gtrid.encode('base64')[:-1]
        bqual = self.bqual.encode('base64')[:-1]
        return "%d_%s_%s" % (int(self.format_id), gtrid, bqual)

    def __getitem__(self, idx):
        if idx < 0:
            idx += 3

        if idx == 0:
            return self.format_id
        elif idx == 1:
            return self.gtrid
        elif idx == 2:
            return self.bqual
        raise IndexError("index out of range")

