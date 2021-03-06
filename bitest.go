package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/july2993/bitest/diff"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var defaultCheckDataTimeout = time.Hour

func setupAutoIncrementAndOffset(db *sql.DB) error {
	var err error
	_, err = db.Exec("drop table if exists auto1;")
	if err != nil {
		return err
	}

	_, err = db.Exec("create table auto1(id bigint primary key auto_increment, uk bigint unique key, v bigint);")
	if err != nil {
		return err
	}

	return nil
}

func cleanupAutoIncrementAndOffset(db *sql.DB) error {
	_, err := db.Exec("drop table if exists auto1;")
	return err
}

func loadAutoIncrementAndOffset(db *sql.DB, n int64, p int, db2 bool) error {
	db.SetMaxIdleConns(p)
	db.SetMaxOpenConns(p)

	var eg errgroup.Group

	var leftN = n
	for i := 0; i < p; i++ {
		eg.Go(func() error {
			for {
				v := atomic.AddInt64(&leftN, -1)
				if v < 0 { // return if have insert n rows
					return nil
				}

				uk := v + 1
				if db2 {
					uk = -uk
				}

				_, err := db.Exec("insert into auto1(uk,v) values(?,?)", uk, v)
				if err != nil {
					return errors.Trace(err)
				}
			}
		})
	}

	err := eg.Wait()
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("finish load data", zap.Int64("number", n))
	return nil
}

func setGloableVar(dsn string, increment int, offset int) error {
	var err error
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("SET @@GLOBAL.auto_increment_increment = %d;", increment))
	if err != nil {
		return errors.Trace(err)
	}

	_, err = db.Exec(fmt.Sprintf("SET @@GLOBAL.auto_increment_offset = %d;", offset))
	if err != nil {
		return errors.Trace(err)
	}

	// see https://github.com/pingcap/tidb/issues/14531#issuecomment-575982919
	time.Sleep(time.Second * 3)
	// check value for new connection
	db.Close()
	db2, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.Trace(err)
	}

	defer db2.Close()

	var v int
	row := db2.QueryRow("SELECT @@auto_increment_increment;")
	err = row.Scan(&v)
	if err != nil {
		return errors.Trace(err)
	}

	if v != increment {
		return errors.Errorf("increment get: %d after set as: %d", v, increment)
	}

	row = db2.QueryRow("SELECT @@auto_increment_offset;")
	err = row.Scan(&v)
	if err != nil {
		return errors.Trace(err)
	}

	if v != offset {
		return errors.Errorf("offset get: %d after set as: %d", v, offset)
	}

	return nil
}

func setupVarAndDB(dsn1 string, dsn2 string, p int, session bool) (db1 *sql.DB, db2 *sql.DB, err error) {
	// setup increment & offset variable
	if session {
		dsn1 += fmt.Sprintf("&auto_increment_increment=%d&auto_increment_offset=%d", 2 /*increment*/, 1 /*offset*/)
		dsn2 += fmt.Sprintf("&auto_increment_increment=%d&auto_increment_offset=%d", 2, 2)
	} else {
		err := setGloableVar(dsn1, 2 /*increment*/, 1 /*offset*/)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		log.Info("set global var success for db1")

		err = setGloableVar(dsn2, 2, 2)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		log.Info("set global var success for db2")
	}

	db1, err = sql.Open("mysql", dsn1)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	db1.SetMaxIdleConns(p)
	db1.SetMaxOpenConns(p)

	db2, err = sql.Open("mysql", dsn2)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	db2.SetMaxIdleConns(p)
	db2.SetMaxOpenConns(p)

	return
}

func testAutoIncrementAndOffset(dsn string, n int64, p int, increment int, offset int, session bool) error {
	if session {
		dsn += fmt.Sprintf("&auto_increment_increment=%d&auto_increment_offset=%d", increment, offset)
	} else {
		err := setGloableVar(dsn, increment, offset)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("set global var success")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()

	err = setupAutoIncrementAndOffset(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = loadAutoIncrementAndOffset(db, n, p, false)
	if err != nil {
		return errors.Trace(err)
	}

	// check auto increment value
	qstr := fmt.Sprintf("select count(*) from auto1 where (id - %d) %% %d = 0", offset, increment)
	row := db.QueryRow(qstr)
	var getn int64
	err = row.Scan(&getn)
	if err != nil {
		return errors.Trace(err)
	}

	if getn != n {
		return errors.Errorf("fail check, expect: %d, but: %d, sql: %s", n, getn, qstr)
	}

	err = cleanupAutoIncrementAndOffset(db)

	return errors.Trace(err)
}

func checkData(timeout time.Duration, db1 *sql.DB, db2 *sql.DB) error {
	start := time.Now()
	df := diff.New(nil, db1, db2)

	for {
		equal, err := df.Equal()
		if err != nil {
			return errors.Trace(err)
		}

		if equal {
			return nil
		}

		if time.Since(start) > timeout {
			return errors.Errorf("failed to check equal")
		}

		time.Sleep(time.Second * 10)
	}
}

// keep inserting and do random add -> change(int -> bigint) -> drop column
func testAddDropColumn(dsn1 string, dsn2 string, p int, session bool) error {
	log.Info("config", zap.String("dsn1", dsn1),
		zap.String("dsn2", dsn2),
		zap.Int("p", p),
		zap.Bool("session", session))

	db1, db2, err := setupVarAndDB(dsn1, dsn2, p, session)
	if err != nil {
		return errors.Trace(err)
	}

	columnTypes := []string{" int default 1", " int not null"}

	for _, cType := range columnTypes {
		cName := "c" + strconv.Itoa(rand.Intn(1000))
		// setup table on db1
		err = setupAutoIncrementAndOffset(db1)
		if err != nil {
			return errors.Trace(err)
		}

		// the table will replicate to db2
		err = checkData(defaultCheckDataTimeout, db1, db2)
		if err != nil {
			return errors.Trace(err)
		}

		keepInsert := func(db *sql.DB, stop chan struct{}) error {
			for {
				// ERROR 1364 (HY000): Field 'c1' doesn't have a default value)
				select {
				case <-stop:
					return nil
				default:
				}

				_, err := db.Exec("insert into auto1(v) values(?)", rand.Int31())
				if err != nil {
					// ERROR 1364 (HY000): Field 'c1' doesn't have a default value)
					// ignore doesn't have default value
					if !strings.Contains(err.Error(), "have a default value") {
						return err
					}
				}

				_, err = db.Exec(fmt.Sprintf("insert into auto1(v, %s) values(?, ?)", cName), rand.Int31(), rand.Int31())
				if err != nil {
					// ignore unknown column
					if !strings.Contains(err.Error(), "unknown column") {
						return err
					}
				}
			}
		}

		startInsert := func() (egp *errgroup.Group, stop chan struct{}) {
			stopInsert := make(chan struct{})
			var eg errgroup.Group
			for i := 0; i < p; i++ {
				eg.Go(func() error {
					return keepInsert(db1, stopInsert)
				})
				eg.Go(func() error {
					return keepInsert(db2, stopInsert)
				})
			}

			return &eg, stopInsert
		}

		var err error

		// add column
		eg, stop := startInsert()
		time.Sleep(time.Second)

		sql := fmt.Sprintf("alter table auto1 add column %s %s;", cName, cType)
		_, err = db1.Exec(sql)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("run ddl success", zap.String("sql", sql))
		time.Sleep(time.Second)

		close(stop)
		err = eg.Wait()
		if err != nil {
			return errors.Trace(err)
		}

		err = checkData(defaultCheckDataTimeout, db1, db2)
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("pass check data equal after add column")

		// change column int -> bigint
		eg, stop = startInsert()
		time.Sleep(time.Second)

		sql = fmt.Sprintf("alter table auto1 modify column %s %s;", cName, strings.Replace(cType, "int", "bigint", -1))
		_, err = db1.Exec(sql)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("run ddl success", zap.String("sql", sql))
		time.Sleep(time.Second)

		close(stop)
		err = eg.Wait()
		if err != nil {
			return errors.Trace(err)
		}

		err = checkData(defaultCheckDataTimeout, db1, db2)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("pass check data equal after change column")

		// drop column
		eg, stop = startInsert()
		time.Sleep(time.Second)

		sql = fmt.Sprintf("alter table auto1 drop column %s;", cName)
		_, err = db1.Exec(sql)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("run ddl success", zap.String("sql", sql))
		time.Sleep(time.Second)

		close(stop)
		err = eg.Wait()
		if err != nil {
			return errors.Trace(err)
		}

		err = checkData(defaultCheckDataTimeout, db1, db2)
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("pass check data equal after drop column")
	}

	return nil
}

func testDML(dsn1 string, dsn2 string, n int64, p int, session bool, opNumber int64) error {
	log.Info("config", zap.String("dsn1", dsn1),
		zap.String("dsn2", dsn2),
		zap.Int64("n", n),
		zap.Int("p", p),
		zap.Bool("session", session),
		zap.Int64("op-number", opNumber))

	db1, db2, err := setupVarAndDB(dsn1, dsn2, p, session)
	if err != nil {
		return errors.Trace(err)
	}

	defer db1.Close()
	defer db2.Close()

	// setup table on db1
	err = setupAutoIncrementAndOffset(db1)
	if err != nil {
		return errors.Trace(err)
	}

	// just in case the downstream's table is dropped and the create table sql is not replicated yet
	// the diff will fail while check the data equals.(It see the table exist but the table is drop when it try to check data).
	time.Sleep(time.Second * 6)

	// the table will replicate to db2
	err = checkData(defaultCheckDataTimeout, db1, db2)
	if err != nil {
		return errors.Trace(err)
	}

	// fill n row
	err = loadAutoIncrementAndOffset(db1, n, p, false)
	if err != nil {
		return errors.Trace(err)
	}

	err = loadAutoIncrementAndOffset(db2, n, p, true)
	if err != nil {
		return errors.Trace(err)
	}

	// check data equal
	err = checkData(defaultCheckDataTimeout, db1, db2)
	if err != nil {
		return errors.Trace(err)
	}

	// do opNumber random insert/delete/update
	doOp := func(db *sql.DB, n int, p int, opNumber int64, db2 bool) error {
		uks := make(map[int]struct{})
		for i := 0; i < n; i++ {
			uks[i] = struct{}{}
		}

		var eg errgroup.Group
		for i := 0; i < p; i++ {
			eg.Go(func() error {
				for {
					if atomic.AddInt64(&opNumber, -1) < 0 {
						return nil
					}

					v := rand.Int()
					uk := rand.Intn(n) + 1
					if db2 {
						uk = -uk
					}

					switch rand.Intn(3) {
					case 0: // try insert
						_, err = db.Exec("replace into auto1(uk, v) values(?, ?)", uk, v)
						if err != nil {
							return errors.Trace(err)
						}
					case 1: // try update
						_, err = db.Exec("update auto1 set v = ? where uk = ?", v, uk)
						if err != nil {
							return errors.Trace(err)
						}
					case 2: // try delete
						_, err = db.Exec("delete from auto1 where uk = ?", uk)
						if err != nil {
							return errors.Trace(err)
						}
					}
				}
			})
		}

		return eg.Wait()
	}

	waitDoOp := make(chan error, 2)
	go func() {
		err := doOp(db1, int(n), p, opNumber, false)
		waitDoOp <- err
	}()
	go func() {
		err := doOp(db2, int(n), p, opNumber, true)
		waitDoOp <- err
	}()

	err = <-waitDoOp
	if err != nil {
		return errors.Trace(err)
	}
	err = <-waitDoOp
	if err != nil {
		return errors.Trace(err)
	}

	// check again
	err = checkData(defaultCheckDataTimeout, db1, db2)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

var rootCmd = &cobra.Command{
	Use:   "bitest",
	Short: "bitest",
}

// Execute runs the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var user string
var password string
var host string
var port int
var n int64
var p int
var increment int
var offset int
var session bool

var user2 string
var password2 string
var host2 string
var port2 int

var opNumber int64
var loop bool

var offsetCmd = &cobra.Command{
	Use:   "offset",
	Short: "validate the correctness of auto_increment_increment & auto_increment_offset",
	Long: `
validate the correctness of auto_increment_increment & auto_increment_offset by:
1, create a table:
	create table auto1(id bigint primary key auto_increment, uk bigint unique key, v bigint);

2, insert n rows with specified increment and offset setted while global or session, then validate the
auto generated column id.

3, check the return value of the flowing query must equal n.
	("select count(*) from auto1 where (id - %d) %% %d = 0", offset, increment)
	`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/test?interpolateParams=true&readTimeout=1m&multiStatements=true", user, password, host, port)

		log.Info("config", zap.Bool("session", session),
			zap.Int("increment", increment),
			zap.Int("offset", offset))
		err := testAutoIncrementAndOffset(dsn, n, p, increment, offset, session)
		if err != nil {
			log.Info("fail test", zap.Error(err))
			return err
		}

		log.Info("test success")
		return nil
	},
}

var dmlCmd = &cobra.Command{
	Use: "dml",
	Long: `
Test correctness of db1 <-> db2 dml replication
Will run all DDL in db1 and let it replicate ddl to db2, so *sync-ddl* should be false in db2.
1, create a table:
	create table auto1(id bigint primary key auto_increment, uk bigint unique key, v bigint);

2, insert n rows with specified increment and offset setted while global or session in both db1 and db2, then check data equal between db1 and db2.

3, try at most op-number random insert/update/delete in both db1 and db2, then check data equal between db1 and db2.

if loop is true will run again and again unless meet some error.
	`,
	RunE: func(cmd *cobra.Command, args []string) error {
		for {
			dsn1 := fmt.Sprintf("%s:%s@tcp(%s:%d)/test?interpolateParams=true&readTimeout=1m&multiStatements=true", user, password, host, port)
			dsn2 := fmt.Sprintf("%s:%s@tcp(%s:%d)/test?interpolateParams=true&readTimeout=1m&multiStatements=true", user2, password2, host2, port2)

			err := testDML(dsn1, dsn2, n, p, session, opNumber)
			if err != nil {
				return errors.Trace(err)
			}

			log.Info("test success")

			if !loop {
				break
			}
		}

		return nil
	},
}

var ddlCmd = &cobra.Command{
	Use: "ddl",
	Long: `
	test some DDL and DML currently and data still consistent finally.
	will add/drop/change column now
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dsn1 := fmt.Sprintf("%s:%s@tcp(%s:%d)/test?interpolateParams=true&readTimeout=1m&multiStatements=true", user, password, host, port)
		dsn2 := fmt.Sprintf("%s:%s@tcp(%s:%d)/test?interpolateParams=true&readTimeout=1m&multiStatements=true", user2, password2, host2, port2)
		err := testAddDropColumn(dsn1, dsn2, p, session)
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("test add/change(int -> bigint)/drop column success")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(offsetCmd)
	rootCmd.AddCommand(dmlCmd)
	rootCmd.AddCommand(ddlCmd)

	// offsetCmd
	offsetCmd.Flags().StringVar(&user, "user", "root", "user of db")
	offsetCmd.Flags().StringVar(&password, "psw", "", "password of db")
	offsetCmd.Flags().StringVar(&host, "host", "127.0.0.1", "host of db")
	offsetCmd.Flags().IntVar(&port, "port", 4000, "port of db")
	offsetCmd.Flags().Int64Var(&n, "n", 10000, "how many rows to fill the table")
	offsetCmd.Flags().IntVar(&p, "p", 16, "max open connection to insert concurrently")
	offsetCmd.Flags().IntVar(&offset, "offset", 1, "the value of auto_increment_offset")
	offsetCmd.Flags().IntVar(&increment, "increment", 2, "the value of auto_increment_increment")
	offsetCmd.Flags().BoolVar(&session, "session", true, "set the variable by session or not")

	// dmlCmd
	dmlCmd.Flags().StringVar(&user, "user", "root", "user of db")
	dmlCmd.Flags().StringVar(&password, "psw", "", "password of db")
	dmlCmd.Flags().StringVar(&host, "host", "127.0.0.1", "host of db")
	dmlCmd.Flags().IntVar(&port, "port", 4000, "port of db")

	dmlCmd.Flags().StringVar(&user2, "user2", "root", "user of db")
	dmlCmd.Flags().StringVar(&password2, "psw2", "", "password of db")
	dmlCmd.Flags().StringVar(&host2, "host2", "127.0.0.1", "host of db")
	dmlCmd.Flags().IntVar(&port2, "port2", 5000, "port of db")

	dmlCmd.Flags().Int64Var(&n, "n", 10000, "how many rows fill up table")
	dmlCmd.Flags().IntVar(&p, "p", 16, "max open connection to db concurrently")
	// dmlCmd.Flags().IntVar(&offset, "offset", 1, "the value of auto_increment_offset")
	// dmlCmd.Flags().IntVar(&increment, "increment", 2, "the value of auto_increment_increment")
	dmlCmd.Flags().BoolVar(&session, "session", true, "set the variable by session or not")
	dmlCmd.Flags().Int64Var(&opNumber, "op-number", 10000, "random number of Insert/Update/delete after filling n rows")
	dmlCmd.Flags().BoolVar(&loop, "loop", false, "run test in loop only quit if meet error")

	// ddlCmd
	ddlCmd.Flags().StringVar(&user, "user", "root", "user of db")
	ddlCmd.Flags().StringVar(&password, "psw", "", "password of db")
	ddlCmd.Flags().StringVar(&host, "host", "127.0.0.1", "host of db")
	ddlCmd.Flags().IntVar(&port, "port", 4000, "port of db")

	ddlCmd.Flags().StringVar(&user2, "user2", "root", "user of db")
	ddlCmd.Flags().StringVar(&password2, "psw2", "", "password of db")
	ddlCmd.Flags().StringVar(&host2, "host2", "127.0.0.1", "host of db")
	ddlCmd.Flags().IntVar(&port2, "port2", 5000, "port of db")

	ddlCmd.Flags().IntVar(&p, "p", 16, "max open connection to db concurrently")
	ddlCmd.Flags().BoolVar(&session, "session", true, "set the variable by session or not")
}

func main() {
	Execute()
}
