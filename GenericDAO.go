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

func (dao *GenericDAO) SetDB(db *sql.DB) { //{{{
	dao.db = db
}                                                 //}}}
func (dao *GenericDAO) Begin() (*sql.Tx, error) { //{{{
	return dao.db.Begin()
}                                       //}}}
func (dao GenericDAO) GetDB() *sql.DB { //{{{
	return dao.db
}                                                                             //}}}
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
} //}}}

func GetInsertSQL(do IGenericDO) (string, []string) { //{{{
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
func GetInsertAllSQL(table string, size int) string { //{{{
	sql := "insert into " + table

	var columns []string
	for i := 0; i < size; i++ {
		columns = append(columns, "?")
	}
	sql += " values (" + strings.Join(columns, ",") + ")"

	return sql
}                                                     //}}}
func GetUpdateSQL(do IGenericDO) (string, []string) { //{{{
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
func GetDeleteSQL(do IGenericDO) (string, []string) { //{{{
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
func GetSelectSQL(do IGenericDO) (string, []string) { //{{{
	sql := "select * from " + do.GetTable() + " where "
	pkeys := do.GetPKeys()

	var condition, values []string
	for k := range pkeys {
		values = append(values, k)
		condition = append(condition, k+" = ? ")
	}
	sql += strings.Join(condition, " and ")
	return sql, values
}                                                                                              //}}}
func (dao GenericDAO) InsertAll(tx *sql.Tx, table string, data []interface{}) (int64, error) { //{{{
	sql := GetInsertAllSQL(table, len(data))
	stmt, err := tx.Prepare(sql)

	if err != nil {
		return 0, err
	}

	result, err := stmt.Exec(data...)
	count, _ := result.RowsAffected()
	defer stmt.Close()
	if err != nil {
		return 0, err
	}

	return count, nil
}                                                                        //}}}
func (dao GenericDAO) Insert(tx *sql.Tx, do IGenericDO) (int64, error) { //{{{
	sql, values := GetInsertSQL(do)
	stmt, err := tx.Prepare(sql)

	if err != nil {
		return 0, err
	}

	data := do.GetDelta()
	var args []interface{}

	for _, v := range values {
		args = append(args, data[v])
	}
	//log.Println(sql)
	//log.Println(args)
	result, err := stmt.Exec(args...)
	defer stmt.Close()
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()

	return count, nil
}                                                                        //}}}
func (dao GenericDAO) Update(tx *sql.Tx, do IGenericDO) (int64, error) { //{{{
	sql, columns := GetUpdateSQL(do)

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return 0, err
	}

	data := do.GetDelta()
	var args []interface{}

	for _, v := range columns {
		args = append(args, data[v])
	}
	//log.Println(sql)
	//log.Println(args)
	result, err := stmt.Exec(args...)
	defer stmt.Close()

	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()

	return count, nil
}                                                                        //}}}
func (dao GenericDAO) Delete(tx *sql.Tx, do IGenericDO) (int64, error) { //{{{
	sql, columns := GetDeleteSQL(do)

	//log.Println(sql)
	stmt, err := tx.Prepare(sql)

	if err != nil {
		return 0, err
	}

	data := do.GetPKeys()
	var args []interface{}
	//log.Println(data)

	for _, v := range columns {
		args = append(args, data[v])
	}
	result, err := stmt.Exec(args...)
	defer stmt.Close()

	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()

	return count, nil
}                                                           //}}}
func (dao GenericDAO) Select(do IGenericDO) (bool, error) { //{{{
	sqlstr, values := GetSelectSQL(do)
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
}                                                                                                                                                              //}}}
func (dao GenericDAO) SelectAllList(table string, conditions map[string]interface{}, orders []string, sort string) (ret []map[string]interface{}, err error) { //{{{
	sql := "select * "
	return dao.SelectList(sql, table, conditions, nil, orders, sort)
}                                                                                                                                                                           //}}}
func (dao GenericDAO) SelectList(sqlstr, table string, conditions map[string]interface{}, groups, orders []string, sort string) (ret []map[string]interface{}, err error) { //{{{
	sqlstr += " from " + table

	var sql_conditions, sql_orders, sql_groups []string
	var args []interface{}
	if len(conditions) > 0 {
		for k, v := range conditions {
			sql_conditions = append(sql_conditions, k+" = ?")
			args = append(args, v)
		}
	}
	if len(groups) > 0 {
		for _, v := range groups {
			sql_groups = append(sql_groups, v)
		}
	}
	if len(orders) > 0 {
		for _, v := range orders {
			sql_orders = append(sql_orders, v)
		}
	}
	if len(sql_conditions) > 0 {
		sqlstr += " where " + strings.Join(sql_conditions, " and ")
	}
	if len(sql_groups) > 0 {
		sqlstr += " group by " + strings.Join(sql_groups, ",")
	}
	if len(sql_orders) > 0 {
		sqlstr += " order by " + strings.Join(sql_orders, ",")
	}

	if sort != "" {
		sqlstr += " " + sort
	}
	//log.Println(sqlstr)
	stmt, err := dao.db.Prepare(sqlstr)

	if err != nil {
		return nil, err
	}

	var rows *sql.Rows
	rows, err = stmt.Query(args...)
	defer stmt.Close()

	if err != nil {
		return nil, err
	}
	ret = dao.setRow(rows)

	return ret, nil
} //}}}
