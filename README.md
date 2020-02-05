Toolkits to test replication

### bitest offset

```shell
➜  bitest git:(master) ✗ ./bitest offset -h

validate the correctness of auto_increment_increment & auto_increment_offset by:
1, create a table:
        create table auto1(id bigint primary key auto_increment, uk bigint unique key, v bigint);

2, insert n rows with specified increment and offset setted while global or session, then validate the
auto generated column id.

3, check the return value of the flowing query must equal n.
        ("select count(*) from auto1 where (id - %d) %% %d = 0", offset, increment)

Usage:
  bitest offset [flags]

Flags:
  -h, --help            help for offset
      --host string     host of db (default "127.0.0.1")
      --increment int   the value of auto_increment_increment (default 2)
      --n int           how many rows to fill the table (default 10000)
      --offset int      the value of auto_increment_offset (default 1)
      --p int           max open connection to insert concurrently (default 16)
      --port int        port of db (default 4000)
      --psw string      password of db
      --session         set the variable by session or not (default true)
      --user string     user of db (default "root")
```


### bitest dml

```shell
➜  bitest git:(master) ✗ ./bitest dml -h

Test correctness of db1 <-> db2 dml replication
Will run all DDL in db1 and let it replicate ddl to db2, so *sync-ddl* should be false in db2.
1, create a table:
        create table auto1(id bigint primary key auto_increment, uk bigint unique key, v bigint);

2, insert n rows with specified increment and offset setted while global or session in both db1 and db2, then check data equal between db1 and db2.

3, try at most op-number random insert/update/delete in both db1 and db2, then check data equal between db1 and db2.

if loop is true will run again and again unless meet some error.

Usage:
  bitest dml [flags]

Flags:
  -h, --help            help for dml
      --host string     host of db (default "127.0.0.1")
      --host2 string    host of db (default "127.0.0.1")
      --loop            run test in loop only quit if meet error
      --n int           how many rows fill up table (default 10000)
      --op-number int   random number of Insert/Update/delete after filling n rows (default 10000)
      --p int           max open connection to db concurrently (default 16)
      --port int        port of db (default 4000)
      --port2 int       port of db (default 5000)
      --psw string      password of db
      --psw2 string     password of db
      --session         set the variable by session or not (default true)
      --user string     user of db (default "root")
      --user2 string    user of db (default "root")
```

### bitest ddl

```
➜  bitest git:(master) ✗ ./bitest ddl -h

        test some DDL and DML currently and data still consistent finally.
        will add/drop/change column now

Usage:
  bitest ddl [flags]

Flags:
  -h, --help           help for ddl
      --host string    host of db (default "127.0.0.1")
      --host2 string   host of db (default "127.0.0.1")
      --p int          max open connection to db concurrently (default 16)
      --port int       port of db (default 4000)
      --port2 int      port of db (default 5000)
      --psw string     password of db
      --psw2 string    password of db
      --session        set the variable by session or not (default true)
      --user string    user of db (default "root")
      --user2 string   user of db (default "root")
```
