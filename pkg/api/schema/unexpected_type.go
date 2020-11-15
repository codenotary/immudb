package schema

type AtomicOperation_Unexpected struct {
	myStruct *struct{}
}

func (*AtomicOperation_Unexpected) isAtomicOperation_Operation() {}
