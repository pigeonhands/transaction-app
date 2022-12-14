## Transaction app

This is a small app to process transactions in a csv file and print the result of the clients to stdout.

for example, with `transactions.csv`
```
type, client, tx, amount
deposit, 1, 1, 1.0
deposit, 2, 2, 2.0
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.0
```

will print 

```
client,available,held,total,
1,1.5,0.0,1.5,false
2,2.0,0.0,2.0,false
```
to stdout.


The transactions and client state are stored in memory so the same state will **NOT** be used across diffrent transaction csv files.

## Assumptions
---
1) The `client` in the `dispute`, `resolve` and `chargeback` transaction is the client performing the `dispute`
2) Locked accounts can not perform any action.

## Larger transactions / Keeping state
-------

Although the apps current usage does not benefit from `async`/`await` as it is taking transactions from a single io source, the `TransactionService` has been written in an `async` way as to possably be used with other io sources such as a tcp stream.

The application is using `sqlite` to process the transactions. For larger transaction files, another database such as `PostgreSQL` should be used.


