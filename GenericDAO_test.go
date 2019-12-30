package zdao

import (
	"testing"
)

func TestSelectList(t *testing.T) { //{{{
	dao := new(GenericDAO)
	dao.SetDriver(OCI8)
	sqlstr := "select * "
	table := "tbl"
	c1 := Condition{Key: "k1", Operator: EQ, Value: 3}
	c2 := Condition{Key: "k2", Operator: NE, Value: 3}
	c3 := Condition{Key: "k3", Operator: IN, Value: "bbb,凹中,5"}
	c4 := Condition{Key: "k4", Operator: NI, Value: "a,b,c"}
	conditions := []Condition{c1, c2, c3, c4}
	ret, arg := dao.GetSelectListSQL(sqlstr, table, conditions, nil, nil, "")
	t.Log(ret)
	t.Log(arg)
	if ret == "select *  from tbl where k1 = :1 and k2 != :2 and k3 in (:3,:4,:5) and k4 not in (:6,:7,:8)" {
		t.Log("GetSelectSQL Testing OK")
	} else {
		t.Error("GetSelectSQL Testing fail!!")
	}
} //}}}
