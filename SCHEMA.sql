CREATE TABLE IF NOT EXISTS [Clients] (
    id          INTEGER PRIMARY KEY,
    available   REAL NOT NULL,
    held        REAL NOT NULL,
    locked      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS [Transactions] (
    id                  INTEGER PRIMARY KEY,
    [type]              TEXT NOT NULL,
    client_id              INTEGER NOT NULL,
    amount              REAL,
	FOREIGN KEY(client_id) REFERENCES Clients(id)
);

CREATE TABLE IF NOT EXISTS [Disputes] (
    transaction_id INTEGER PRIMARY KEY,
	FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
);