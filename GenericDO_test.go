package zdao

import (
	"testing"
)

func TestSetPkey(t *testing.T) { //{{{
	do := new(GenericDO)
	do.Init("table")
	do.SetPKey("a", 3)
	ret := do.GetPKey("a")
	if ret == 3 {
		t.Log("SetPkey testing ok")
	} else {
		t.Error("SetPkey Testing fail!!")
	}
}                                //}}}
func TestSetData(t *testing.T) { //{{{
	do := new(GenericDO)
	do.Init("table")
	do.Set("a", "bbb")
	ret := do.Get("a")
	if ret == "bbb" {
		t.Log("Set Testing OK")
	} else {
		t.Error("Set Testing fail!!")
	}
}                                 //}}}
func TestGetPkeys(t *testing.T) { //{{{
	do := new(GenericDO)
	do.Init("table")
	do.SetPKey("a", "bbb")
	do.SetPKey("b", 1)
	pkeys := do.GetPKeys()
	if pkeys["a"] == "bbb" {
		t.Log("GetPKeys Testing OK")
	} else {
		t.Error("GetPKeys Testing fail!!")
	}
}                                //}}}
func TestGetData(t *testing.T) { //{{{
	do := new(GenericDO)
	do.Init("table")
	do.Set("a", "bbb")
	do.Set("b", 1)
	pkeys := do.GetData()
	if pkeys["b"] == 1 {
		t.Log("GetData Testing OK")
	} else {
		t.Error("GetData Testing fail!!")
	}
}                                     //}}}
func TestGetSelectSQL(t *testing.T) { //{{{
	do := new(GenericDO)
	do.Init("table")
	do.SetPKey("column_a", "a")
	do.SetPKey("column_b", 2)
	ret, _ := getSelectSQL(do)
	t.Log(ret)
	if ret == "select * from table where column_a = ?  and column_b = ? " {
		t.Log("GetSelectSQL Testing OK")
	} else {
		t.Error("GetSelectSQL Testing fail!!")
	}
}                                  //}}}
func TestInsertSQL(t *testing.T) { //{{{
	do := new(GenericDO)
	do.Init("table")
	do.SetPKey("column_a", "a")
	do.Set("column_b", 2)
	ret, _ := getInsertSQL(do)
	t.Log("[" + ret + "]")
	if ret == "insert into table (column_a,column_b) values (?,?)" {
		t.Log("GetInsertSQL Testing OK")
	} else {
		t.Error("GetInsertSQL Testing fail!!")
	}
}                                  //}}}
func TestUpdateSQL(t *testing.T) { //{{{
	do := new(GenericDO)
	do.Init("table")
	do.SetPKey("column_a", "a")
	do.Set("column_b", 2)
	ret, _ := getUpdateSQL(do)
	t.Log(ret)
	if ret == "update table set column_b = ? where column_a = ?" {
		t.Log("GetUpdateSQL Testing OK")
	} else {
		t.Error("GetUpdateSQL Testing fail!!")
	}
}                                  //}}}
func TestDeleteSQL(t *testing.T) { //{{{
	do := new(GenericDO)
	do.Init("table")
	do.SetPKey("column_a", "a")
	do.SetPKey("column_b", 2)
	ret, _ := getDeleteSQL(do)
	t.Log(ret)
	if ret == "delete from table where column_a = ? and column_b = ?" {
		t.Log("GetDeleteSQL Testing OK")
	} else {
		t.Error("GetDeleteSQL Testing fail!!")
	}
} //}}}
