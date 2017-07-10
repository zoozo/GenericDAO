package zdao

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	//"log"
	"strings"
)

type IGenericDO interface {
	//	Init(table string)
	GetTable() string
	//	SetPKey(key string, value interface{})
	Set(key string, value interface{})
	GetPKey(key string) interface{}
	Get(key string) interface{}
	GetPKeys() map[string]interface{}
	GetData() map[string]interface{}
	GetDelta() map[string]interface{}
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
	data := do.GetDelta()

	var columns, values []string
	for k := range data {
		values = append(values, k)
		columns = append(columns, "?")
	}
	sql += " (" + strings.Join(values, ",") + ") values (" + strings.Join(columns, ",") + ")"

	return sql, values
}                                                     //}}}
func getUpdateSQL(do IGenericDO) (string, []string) { //{{{
	sql := "update " + do.GetTable()
	sql += " set "
	var columns, values []string

	data := do.GetData()
	for k := range data {
		values = append(values, k)
		columns = append(columns, k+" = ?")
	}
	sql += strings.Join(columns, ",") + " where "

	keys := do.GetPKeys()
	columns = nil
	for k := range keys {
		values = append(values, k)
		columns = append(columns, k+" = ?")
	}
	sql += strings.Join(columns, " and ")

	return sql, values
}                                                     //}}}
func getDeleteSQL(do IGenericDO) (string, []string) { //{{{
	sql := "delete from " + do.GetTable() + " where "
	var columns, values []string

	keys := do.GetPKeys()
	for k := range keys {
		values = append(values, k)
		columns = append(columns, k+" = ?")
	}
	sql += strings.Join(columns, " and ")

	return sql, values
}                                                     //}}}
func getSelectSQL(do IGenericDO) (string, []string) { //{{{
	sql := "select * from " + do.GetTable() + " where "
	pkeys := do.GetPKeys()

	var condition, values []string
	for k := range pkeys {
		values = append(values, k)
		condition = append(condition, k+" = ? ")
	}
	sql += strings.Join(condition, " and ")
	return sql, values
}                                         //}}}
func (dao *GenericDAO) Init(db *sql.DB) { //{{{
	dao.db = db
} //}}}
func (dao *GenericDAO) Begin() (*sql.Tx, error) {
	return dao.db.Begin()

}
func (dao GenericDAO) setRow(rows *sql.Rows) (ret []map[string]interface{}) { //{{{
	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		valuePtrs[0] = &values[0]
		for i, _ := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)

		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		ret = append(ret, entry)
	}

	return ret
}                                                                       //}}}
func (dao GenericDAO) Insert(tx *sql.Tx, do IGenericDO) (bool, error) { //{{{
	sql, values := getInsertSQL(do)
	stmt, err := tx.Prepare(sql)

	if err != nil {
		return false, err
	}

	data := do.GetData()
	var args []interface{}

	for _, v := range values {
		args = append(args, data[v])
	}
	_, err = stmt.Exec(args...)
	if err != nil {
		return false, err
	}

	stmt.Close()

	return true, nil
}                                                                       //}}}
func (dao GenericDAO) Update(tx *sql.Tx, do IGenericDO) (bool, error) { //{{{
	sql, columns := getUpdateSQL(do)

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return false, err
	}

	data := do.GetDelta()
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
}                                                                       //}}}
func (dao GenericDAO) Delete(tx *sql.Tx, do IGenericDO) (bool, error) { //{{{
	sql, columns := getDeleteSQL(do)

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return false, err
	}

	data := do.GetPKeys()
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
}                                                           //}}}
func (dao GenericDAO) Select(do IGenericDO) (bool, error) { //{{{
	sqlstr, values := getSelectSQL(do)
	stmt, err := dao.db.Prepare(sqlstr)

	if err != nil {
		return false, err
	}

	pkeys := do.GetPKeys()
	var args []interface{}

	for _, v := range values {
		args = append(args, pkeys[v])
	}
	var rows *sql.Rows
	rows, err = stmt.Query(args...)

	if err != nil {
		return false, err
	}
	ret := dao.setRow(rows)
	if len(ret) <= 0 {
		return false, nil
	}

	for k, v := range ret[0] {
		if _, ok := pkeys[k]; ok {
			continue
		}
		do.Set(k, v)
	}

	stmt.Close()

	return true, nil
} //}}}
