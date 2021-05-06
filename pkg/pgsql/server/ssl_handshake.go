package server

import (
	"crypto/tls"
)

func (s *session) handshake() error {
	var tlsConn *tls.Conn
	tlsConn = tls.Server(s.conn, s.tlsConfig)
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	s.conn = tlsConn
	return nil
}
