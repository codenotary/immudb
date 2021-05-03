package fmessages

type TerminateMsg struct{}

func ParseTerminateMsg(payload []byte) TerminateMsg {
	return TerminateMsg{}
}
