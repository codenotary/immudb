package sessionservice

import (
	"context"
	"fmt"
	"sync"

	"github.com/codenotary/immudb/pkg/client/heartbeater"
	"google.golang.org/grpc/metadata"
)

type SessionService interface {
	AddSession(session Session) error
	GetSession(sessionID string) (Session, error)
	DeleteSession(sessionID string)
	Len() int
	SessionFromCtx(ctx context.Context) (Session, error)
}

type Session struct {
	SessionID string
	Database  string
	Hearbeat  heartbeater.HeartBeater
}

type sessionService struct {
	sync.RWMutex
	sessions map[string]*Session
}

func NewSessionService() SessionService {
	return &sessionService{
		sessions: map[string]*Session{},
	}
}

func (ss *sessionService) AddSession(session Session) error {
	ss.Lock()
	defer ss.Unlock()

	if _, ok := ss.sessions[session.SessionID]; ok {
		return fmt.Errorf("session already exists")
	}

	ss.sessions[session.SessionID] = &session

	return nil
}

func (ss *sessionService) GetSession(sessionID string) (Session, error) {
	ss.Lock()
	defer ss.Unlock()

	//hack for now
	if sessionID == "" && len(ss.sessions) == 1 {
		for _, v := range ss.sessions {
			return *v, nil
		}
	}

	if v, ok := ss.sessions[sessionID]; ok {
		return *v, nil
	}

	return Session{}, fmt.Errorf("no session id: %s", sessionID)
}

func (ss *sessionService) DeleteSession(sessionID string) {
	ss.Lock()
	defer ss.Unlock()

	delete(ss.sessions, sessionID)
}

func (ss *sessionService) Len() int {
	ss.Lock()
	defer ss.Unlock()

	return len(ss.sessions)
}

func (ss *sessionService) SessionFromCtx(ctx context.Context) (Session, error) {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if sessionIDs := md.Get("sessionid"); len(sessionIDs) > 0 {
			return ss.GetSession(sessionIDs[0])
		}
	}

	return Session{}, fmt.Errorf("sessionID not found")
}
