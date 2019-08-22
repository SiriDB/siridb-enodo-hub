import datetime


class Client:

    def __init__(self, client_id, ip_address, writer, version="unknown", last_seen=None, busy=None):
        self.client_id = client_id
        self.ip_address = ip_address
        self.writer = writer
        self.busy = busy
        self.last_seen = last_seen
        self.version = version
        if last_seen is None:
            self.last_seen = datetime.datetime.now()

    def to_dict(self):
        return {'client_id': self.client_id,
                'ip_address': self.ip_address,
                'writer': self.writer,
                'busy': self.busy,
                'last_seen': self.last_seen,
                'version': self.version}


class ClientManager:
    listeners = {}
    workers = {}

    @classmethod
    async def add_listener(cls, client):
        cls.listeners[client.client_id] = client

    @classmethod
    async def add_worker(cls, client):
        cls.workers[client.client_id] = client

    @classmethod
    async def get_listener_by_id(cls, client_id):
        if client_id in cls.listeners:
            return cls.listeners.get(client_id)
        return None

    @classmethod
    async def get_worker_by_id(cls, client_id):
        if client_id in cls.workers:
            return cls.workers.get(client_id)
        return None

    @classmethod
    async def get_free_worker(cls):
        for worker in cls.workers:
            print(cls.workers.get(worker).busy, not cls.workers.get(worker).busy, cls.workers.get(worker).busy is False)
            if not cls.workers.get(worker).busy:
                return cls.workers.get(worker)
        return None

    @classmethod
    async def check_clients_alive(cls, max_timeout):
        clients_to_remove = []
        for client in cls.listeners:
            if (datetime.datetime.now() - cls.listeners.get(client).last_seen) \
                    .total_seconds() > max_timeout:
                print(f'Not alive: {client}')
                clients_to_remove.append(client)

        for client in clients_to_remove:
            del cls.listeners[client]
        clients_to_remove = []

        for client in cls.workers:
            if (datetime.datetime.now() - cls.workers.get(client).last_seen) \
                    .total_seconds() > max_timeout:
                print(f'Not alive: {client}')
                clients_to_remove.append(client)

        for client in clients_to_remove:
            del cls.workers[client]
