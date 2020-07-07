package pghelper

import (
	"fmt"
	"testing"

	pg "github.com/go-pg/pg/v10"
	"github.com/stretchr/testify/assert"
)

func Test_dbProxy(t *testing.T) {
	db := New()
	db.InitFromURL("postgres://postgres:postgres@localhost:5432/test")
	defer db.Close()
	var s1 string
	conn, err := db.GetConn()
	if err != nil {
		fmt.Println("PostgreSQL GetConn error ", err)
	}
	_, err = conn.QueryOne(pg.Scan(&s1), `SELECT 1`)
	if err != nil {
		fmt.Println("PostgreSQL is down", err)
	}
	assert.Equal(t, "1", s1)

}

func Test_dbProxy_Regist(t *testing.T) {
	db := New()
	db.Regist(func(dbCli *pg.DB) error {
		var s1 string
		_, err := dbCli.QueryOne(pg.Scan(&s1), `SELECT 1`)
		if err != nil {
			fmt.Println("PostgreSQL is down")
			return err
		}
		assert.Equal(t, "1", s1)
		return nil
	})
	db.InitFromURL("postgres://postgres:postgres@localhost:5432/test")
	defer db.Close()
}

func Test_dbProxy_Exec(t *testing.T) {
	db := New()
	db.InitFromURL("postgres://postgres:postgres@localhost:5432/test")
	defer db.Close()
	res, err := db.Exec("SELECT 1")
	if err != nil {
		t.Fatalf("get err %v", err)
	}
	assert.Equal(t, 1, res.RowsReturned())

}
