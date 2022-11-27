CREATE TABLE IF NOT EXISTS [Clients] (
    id          INTEGER PRIMARY KEY,
    available   BIGINT NOT NULL,
    held        BIGINT NOT NULL,
    locked      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS [Transactions] (
    id                      INTEGER PRIMARY KEY,
    [type]                  TEXT NOT NULL,
    client_id              INTEGER NOT NULL,
    amount                  BIGINT,
	FOREIGN KEY(client_id) REFERENCES Clients(id)
);

CREATE TABLE IF NOT EXISTS [Disputes] (
    transaction_id INTEGER PRIMARY KEY,
	FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
);