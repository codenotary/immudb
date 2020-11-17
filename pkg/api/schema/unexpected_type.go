package schema

type Op_Unexpected struct {
	myStruct *struct{}
}

func (*Op_Unexpected) isBatchOp_Operation() {}
