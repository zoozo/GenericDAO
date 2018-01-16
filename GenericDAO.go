package zdao

import (
	"database/sql"
	"log"
	"strconv"
	"strings"
)

type DriverType int

const (
	MYSQL = iota
	MSSQL = iota
	OCI8  = iota
)

const (
	EQ = "EQ"
	IN = "IN"
	NE = "NE"
	NI = "NI"
	GT = "GT"
	GE = "GE"
	LT = "LT"
	LE = "LE"
)

type Condition struct {
	Key      string
	Operator string
	Value    interface{}
}

type IGenericDO interface {
	GetTable() string
	Set(key string, value interface{})
	GetPKey(key string) interface{}
	Get(key string) interface{}
	GetPKeys() map[string]interface{}
	GetData() map[string]interface{}
	GetDelta() map[string]interface{}
}
type GenericDAO struct {
	db     *sql.DB
	driver DriverType
	idx    int
	debug  bool
	args   []interface{}
}

func (dao *GenericDAO) SetDebug() { //{{{
	dao.debug = true
} //}}}
func (dao *GenericDAO) SetDB(db *sql.DB) { //{{{
	dao.db = db
} //}}}
func (dao *GenericDAO) SetArgs(args []interface{}) { //{{{
	//if args != nil {
	dao.args = args
	//}
} //}}}
func (dao *GenericDAO) Begin() (*sql.Tx, error) { //{{{
	return dao.db.Begin()
} //}}}
func (dao *GenericDAO) SetDriver(driverType DriverType) { //{{{
	dao.driver = driverType
} //}}}
func (dao GenericDAO) GetDB() *sql.DB { //{{{
	return dao.db
} //}}}
func (dao GenericDAO) SetRow(rows *sql.Rows) (ret []map[string]interface{}) { //{{{
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

func GetInsertSQL(do IGenericDO, driverType DriverType) (string, []string) { //{{{
	sql := "insert into " + do.GetTable()
	data := do.GetDelta()

	var columns, values []string
	for k := range data {
		values = append(values, k)
		if driverType == OCI8 {
			columns = append(columns, ":"+k)
		} else {
			columns = append(columns, "?")
		}
	}
	sql += " (" + strings.Join(values, ",") + ") values (" + strings.Join(columns, ",") + ")"

	return sql, values
} //}}}
func GetInsertAllSQL(table string, size int, driverType DriverType) string { //{{{
	sql := "insert into " + table

	var columns []string
	for i := 0; i < size; i++ {
		if driverType == OCI8 {
			columns = append(columns, ":"+strconv.Itoa(i))
		} else {
			columns = append(columns, "?")
		}
	}
	sql += " values (" + strings.Join(columns, ",") + ")"

	return sql
} //}}}
func GetUpdateSQL(do IGenericDO, driverType DriverType) (string, []string) { //{{{
	sql := "update " + do.GetTable()
	sql += " set "
	var columns, values []string

	data := do.GetData()
	for k := range data {
		values = append(values, k)
		if driverType == OCI8 {
			columns = append(columns, k+" = :"+k)
		} else {
			columns = append(columns, k+" = ?")
		}
	}
	sql += strings.Join(columns, ",") + " where "

	keys := do.GetPKeys()
	columns = nil
	for k := range keys {
		values = append(values, k)
		if driverType == OCI8 {
			columns = append(columns, k+" = :"+k)
		} else {
			columns = append(columns, k+" = ?")
		}
	}
	sql += strings.Join(columns, " and ")

	return sql, values
} //}}}
func GetDeleteSQL(do IGenericDO, driverType DriverType) (string, []string) { //{{{
	sql := "delete from " + do.GetTable() + " where "
	var columns, values []string

	keys := do.GetPKeys()
	for k := range keys {
		values = append(values, k)
		if driverType == OCI8 {
			columns = append(columns, k+" = :"+k)
		} else {
			columns = append(columns, k+" = ?")
		}
	}
	sql += strings.Join(columns, " and ")

	return sql, values
} //}}}
func GetSelectSQL(do IGenericDO, driverType DriverType) (string, []string) { //{{{
	sql := "select * from " + do.GetTable() + " where "
	pkeys := do.GetPKeys()

	var condition, values []string
	for k := range pkeys {
		values = append(values, k)
		if driverType == OCI8 {
			condition = append(condition, k+" = :"+k)
		} else {
			condition = append(condition, k+" = ? ")
		}
	}
	sql += strings.Join(condition, " and ")
	return sql, values
} //}}}
func (dao GenericDAO) InsertAll(tx *sql.Tx, table string, data []interface{}) (int64, error) { //{{{
	sql := GetInsertAllSQL(table, len(data), dao.driver)
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
} //}}}
func (dao GenericDAO) Insert(tx *sql.Tx, do IGenericDO) (int64, error) { //{{{
	sql, values := GetInsertSQL(do, dao.driver)
	stmt, err := tx.Prepare(sql)

	if err != nil {
		return 0, err
	}

	data := do.GetDelta()
	var args []interface{}

	for _, v := range values {
		args = append(args, data[v])
	}
	if dao.debug {
		log.Println("insert sql:", sql)
		log.Println("insert args:", args)
	}
	result, err := stmt.Exec(args...)
	defer stmt.Close()
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()

	return count, nil
} //}}}
func (dao GenericDAO) Update(tx *sql.Tx, do IGenericDO) (int64, error) { //{{{
	sql, columns := GetUpdateSQL(do, dao.driver)

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return 0, err
	}

	data := do.GetDelta()
	var args []interface{}

	for _, v := range columns {
		args = append(args, data[v])
	}
	if dao.debug {
		log.Println("SQL:", sql)
		log.Println("ARGS:", args)
	}
	result, err := stmt.Exec(args...)
	defer stmt.Close()

	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()

	return count, nil
} //}}}
func (dao GenericDAO) Delete(tx *sql.Tx, do IGenericDO) (int64, error) { //{{{
	sql, columns := GetDeleteSQL(do, dao.driver)

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
} //}}}
func (dao GenericDAO) SelectWithTx(tx *sql.Tx, do IGenericDO) (bool, error) { //{{{
	sqlstr, values := GetSelectSQL(do, dao.driver)
	stmt, err := tx.Prepare(sqlstr)

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
	ret := dao.SetRow(rows)
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
func (dao GenericDAO) Select(do IGenericDO) (bool, error) { //{{{
	sqlstr, values := GetSelectSQL(do, dao.driver)
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
	if dao.debug {
		log.Println("sql:", sqlstr)
		log.Println("args:", args)
	}
	rows, err = stmt.Query(args...)

	if err != nil {
		return false, err
	}
	ret := dao.SetRow(rows)
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
func (dao *GenericDAO) SelectAllList(table string, conditions []Condition, orders []string, sort string) (ret []map[string]interface{}, err error) { //{{{
	sql := "select * "
	return dao.SelectList(sql, table, conditions, nil, orders, sort)
} //}}}
func (dao *GenericDAO) getBindString() string { //{{{
	if dao.driver == OCI8 {
		dao.idx++
		return ":" + strconv.Itoa(dao.idx)
	} else {
		return "?"
	}
} //}}}
func (dao *GenericDAO) Arrange(c *Condition, sql_conditions *[]string, args *[]interface{}) { //{{{
	key := c.Key
	if c.Operator == EQ || c.Operator == NE || c.Operator == GT || c.Operator == GE || c.Operator == LT || c.Operator == LE {
		switch c.Operator {
		case EQ:
			key += " = " + dao.getBindString()
		case NE:
			key += " != " + dao.getBindString()
		case GT:
			key += " > " + dao.getBindString()
		case GE:
			key += " >= " + dao.getBindString()
		case LT:
			key += " < " + dao.getBindString()
		case LE:
			key += " <= " + dao.getBindString()
		}
		*sql_conditions = append(*sql_conditions, key)
		*args = append(*args, c.Value)
	} else if c.Operator == NI || c.Operator == IN {
		values := strings.Split(c.Value.(string), ",")
		if len(values) > 0 {
			var keys []string
			for _, v := range values {
				*args = append(*args, v)
				keys = append(keys, dao.getBindString())
			}
			if c.Operator == IN {
				key += " in (" + strings.Join(keys, ",") + ")"
			} else {
				key += " not in (" + strings.Join(keys, ",") + ")"
			}
			*sql_conditions = append(*sql_conditions, key)
		}
	}
} //}}}
func (dao GenericDAO) GetSelectListSQL(sqlstr, table string, conditions []Condition, groups, orders []string, sort string, limit ...int) (string, []interface{}) { //{{{
	dao.idx = 0
	sqlstr += " from " + table

	var sql_conditions, sql_orders, sql_groups []string
	var args []interface{}
	if len(conditions) > 0 {
		for _, v := range conditions {
			dao.Arrange(&v, &sql_conditions, &args)

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
	if len(limit) == 1 {
		sqlstr += " limit " + strconv.Itoa(limit[0])
	} else if len(limit) == 2 {
		sqlstr += " limit " + strconv.Itoa(limit[1]) + "," + strconv.Itoa(limit[0])
	}

	//log.Println(sqlstr)
	if dao.debug {
		log.Println("sql:", sqlstr)
		log.Println("args:", args)
	}
	return sqlstr, args
} //}}}
func (dao *GenericDAO) SelectList(sqlstr, table string, conditions []Condition, groups, orders []string, sort string, limit ...int) (ret []map[string]interface{}, err error) { //{{{
	sqlstr, args := dao.GetSelectListSQL(sqlstr, table, conditions, groups, orders, sort, limit...)
	stmt, err := dao.db.Prepare(sqlstr)

	if err != nil {
		return nil, err
	}

	var rows *sql.Rows
	if dao.args == nil {
		dao.args = args
	} else {
		dao.args = append(dao.args, args...)
	}
	if dao.debug {
		log.Println("SelectList sql:", sqlstr)
		log.Println("SelectList args:", dao.args)
	}
	rows, err = stmt.Query(dao.args...)
	dao.args = nil

	defer stmt.Close()

	if err != nil {
		return nil, err
	}
	ret = dao.SetRow(rows)

	return ret, nil
} //}}}
func (dao GenericDAO) SelectAllList2(table string, conditions map[string]interface{}, orders []string, sort string) (ret []map[string]interface{}, err error) { //{{{
	sql := "select * "
	return dao.SelectList2(sql, table, conditions, nil, orders, sort)
} //}}}
func (dao GenericDAO) SelectList2(sqlstr, table string, conditions map[string]interface{}, groups, orders []string, sort string, limit ...string) (ret []map[string]interface{}, err error) { //{{{
	sqlstr += " from " + table

	var sql_conditions, sql_orders, sql_groups []string
	var args []interface{}
	if len(conditions) > 0 {
		for k, v := range conditions {
			if dao.driver == OCI8 {
				sql_conditions = append(sql_conditions, k+" = :"+k)
			} else {
				sql_conditions = append(sql_conditions, k+" = ?")
			}
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
	if len(limit) == 1 {
		sqlstr += " limit " + limit[0]
	} else if len(limit) == 2 {
		sqlstr += " limit " + limit[1] + "," + limit[0]
	}

	//log.Println(sqlstr)
	if dao.debug {
		log.Println("sql:", sqlstr)
		log.Println("args:", args)
	}
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
	ret = dao.SetRow(rows)

	return ret, nil
} //}}}
