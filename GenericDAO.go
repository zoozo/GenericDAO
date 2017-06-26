package zdao

import (
	"database/sql"
	//"errors"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

type IGenericDO interface {
	//	Init(table string)
	GetTable() string
	//	SetPKey(key string, value interface{})
	//	Set(key string, value interface{})
	GetPKey(key string) interface{}
	Get(key string) interface{}
	GetPKeys() map[string]interface{}
	GetData() map[string]interface{}
}
type GenericDAO struct {
	db *sql.DB
}

func (dao *GenericDAO) SetDB(db *sql.DB) {
	dao.db = db
}
func (dao *GenericDAO) NewDB(user, password, ip, port string) error {
	var err error
	dao.db, err = sql.Open("mysql", user+":"+password+"@tcp("+ip+":"+port+")/pms?charset=utf8")
	return err
}
func getInsertSQL(do IGenericDO) (string, []string) { //{{{
	sql := "insert into " + do.GetTable()
	data := do.GetData()

	var columns, values []string
	for k := range data {
		columns = append(columns, k)
		values = append(values, "?")
	}
	sql += " (" + strings.Join(columns, ",") + ") values (" + strings.Join(values, ",") + ")"

	return sql, columns
}                                         //}}}
func getSelectSQL(do IGenericDO) string { //{{{
	sql := "select * from " + do.GetTable() + " where "
	pkeys := do.GetPKeys()

	var condition []string
	for k := range pkeys {
		condition = append(condition, k+" = ? ")
	}
	sql += strings.Join(condition, " and ")
	return sql
}                                         //}}}
func (dao *GenericDAO) Init(db *sql.DB) { //{{{
	dao.db = db
} //}}}
func (dao *GenericDAO) Begin() (*sql.Tx, error) {
	return dao.db.Begin()

}
func (dao GenericDAO) Select(do IGenericDO) string { //{{{
	return getSelectSQL(do)
}                                                                       //}}}
func (dao GenericDAO) Insert(tx *sql.Tx, do IGenericDO) (bool, error) { //{{{
	sql, columns := getInsertSQL(do)
	stmt, err := tx.Prepare(sql)

	if err != nil {
		return false, err
	}

	data := do.GetData()
	var args []interface{}

	for _, v := range columns {
		args = append(args, data[v])
	}
	_, err = stmt.Exec(args...)
	if err != nil {
		return false, err
	}

	stmt.Close()

	return true, nil
} //}}}
