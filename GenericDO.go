package zdao

type GenericDO struct {
	pkeys map[string]interface{}
	data  map[string]interface{}
	delta map[string]interface{}
	table string
}

func (do *GenericDO) Init(table string) {
	do.pkeys = make(map[string]interface{})
	do.data = make(map[string]interface{})
	do.delta = make(map[string]interface{})
	do.table = table
}
func (do GenericDO) GetTable() string {
	return do.table
}
func (do *GenericDO) SetPKey(key string, value interface{}) {
	do.pkeys[key] = value
	do.delta[key] = value
}
func (do *GenericDO) Set(key string, value interface{}) {
	do.data[key] = value
	do.delta[key] = value
}
func (do *GenericDO) SetData(key string, value interface{}) {
	do.data[key] = value
	do.delta[key] = value
}
func (do GenericDO) GetPKey(key string) interface{} {
	return do.pkeys[key]
}
func (do GenericDO) Get(key string) interface{} {
	return do.data[key]
}
func (do GenericDO) GetPKeys() map[string]interface{} {
	return do.pkeys
}
func (do GenericDO) GetData() map[string]interface{} {
	return do.data
}
func (do GenericDO) GetDelta() map[string]interface{} {
	return do.delta
}
