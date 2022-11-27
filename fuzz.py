import subprocess
import random
import csv
from decimal import *

BASIC_TX = ["deposit", "withdrawal"]
DISPUTE_TX =  ["dispute", "resolve", "chargeback"]
class Transaction():
    def __init__(self, client, type, tx, amount):
        self.client = client
        self.type = type
        self.tx = tx
        self.amount =Decimal(amount)
        self.disputed = False

    @classmethod
    def random(cls, tx):

        t = random.choice(BASIC_TX+DISPUTE_TX)

        tx_num = int(tx if t in BASIC_TX else random.randint(0, int(tx)))

        return cls(
            int(random.randint(1, 30)),
            t,
            tx_num,
            Decimal(random.randint(100, 100000)) / 10000,
        )
class Client:
    def __init__(self, id, available, held, locked, _total=None):
        self.id = id
        self.available = Decimal(available)
        self.held = Decimal(held)
        self.locked = locked
        self._total=_total
    
    @property
    def total(self):
        if self._total:
            return self._total
        else:
            return self.available+self.held
    def __repr__(self) -> str:
        return f"Client(id={self.id}, available={self.available}, held={self.held}, locked={self.locked}, total={self.total})"

class Processor:
    def __init__(self) -> None:
        self.transactions = {}
        self.clients = {}

    def process_transaction(self, t: Transaction):
        if t.type in BASIC_TX:
            self.transactions[t.tx] =t
        print(t.type, t.amount)

        m = getattr(self, f"handle_{t.type}")
        m(t)

    def _get_client(self, id):
        c = self.clients.get(id)
        if not c:
            c = Client(id, 0, 0, False)
            self.clients[id] = c
        return c

    def handle_deposit(self, t:Transaction):
        c = self._get_client(t.client)
        if c.locked:
            return
        c.available += t.amount

    def handle_withdrawal(self, t: Transaction):
        c = self._get_client(t.client)
        if c.locked:
            return
        if c.available >= t.amount:
            c.available -= t.amount

    def handle_dispute(self, t: Transaction):
        disputed_tx = self.transactions.get(t.tx)
        if not disputed_tx:
            return
        c = self._get_client(disputed_tx.client)
        if c.locked:
            return
        c.available -= disputed_tx.amount
        c.held += disputed_tx.amount
        disputed_tx.disputed = True
        

    def handle_resolve(self, t: Transaction):
        disputed_tx = self.transactions.get(t.tx)
        if not disputed_tx or not disputed_tx.disputed:
            return

        c = self._get_client(disputed_tx.client)
        if c.locked:
            return
        c.available += disputed_tx.amount
        c.held -= disputed_tx.amount
        disputed_tx.disputed = False

    def handle_chargeback(self, t: Transaction):
        disputed_tx = self.transactions.get(t.tx)
        if not disputed_tx or not disputed_tx.disputed:
            return

        c = self._get_client(disputed_tx.client)
        if c.locked:
            return
        c.held -= disputed_tx.amount
        c.locked = True

def create_csv(fname, p:Processor):
    with open(fname, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['type','client','tx','amount'])
        w.writeheader()
        for tx_id in range(100):
            tx = Transaction.random(tx_id)
            existing = p.transactions.get(tx.tx)
            if existing and existing.disputed:
                continue
            if existing and tx.type == 'dispute' and existing.type != 'deposit':
                continue
            p.process_transaction(tx)
            w.writerow(dict(type=tx.type, client=tx.client, tx=tx.tx, amount=tx.amount))
    
def main():
    TEST_CSV_FILE = "test.csv"
    p = Processor()
    create_csv(TEST_CSV_FILE, p)
    fizz_clients = list(p.clients.values())
    fizz_clients.sort(key=lambda c: c.id)

    proc = subprocess.Popen(f'cargo run -- {TEST_CSV_FILE}', stdout=subprocess.PIPE)
    output = proc.communicate()[0].decode('ascii')
    reader = csv.DictReader(output.splitlines(), delimiter=",")
    output_clients = [Client(int(d['client']), Decimal(d['available']), Decimal(d['held']), d['locked'] == 'true', Decimal(d['total'])) for d in reader]
    output_clients.sort(key=lambda c: c.id)

    for row in output_clients:
        print(row)
    
    fails = 0
    for expected, out in zip(fizz_clients, output_clients):
        print("Expected:", expected)
        print("Got:     ", out)
        print()
        if expected.id != out.id or expected.available != out.available or expected.held != out.held or expected.total != out.total or expected.locked != out.locked:
            print("Failed")
            return

    print("Test passed!")

if __name__ == "__main__":
    main()
