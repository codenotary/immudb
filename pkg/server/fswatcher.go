package server

import (
	"time"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/fsnotify/fsnotify"
)

func setUpWatcher(s *store.Store, dbDir string, log logger.Logger) (*fsnotify.Watcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Warningf("failed to create fs watcher: %v", err)
		return nil, err
	}
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					now := time.Now()
					if now.Sub(s.GetChangedAt()) > 1*time.Second {
						s.SetTamperedAt(now)
						log.Warningf("possible tampering detected on %s", event.Name)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Errorf("db dir %s watcher error received: %v", dbDir, err)
			}
		}
	}()
	return watcher, err
}

func addDirToWatcher(watcher *fsnotify.Watcher, dir string, log logger.Logger) {
	err := watcher.Add(dir)
	if err != nil {
		log.Errorf("error adding %s dir to fs watcher: %v", dir, err)
		return
	}
	log.Infof("dir %s added to fs watcher", dir)
}

func closeWatcher(watcher *fsnotify.Watcher, dir string, log logger.Logger) {
	err := watcher.Close()
	if err != nil {
		log.Errorf("error closing fs watcher on dir %s: %v", dir, err)
		return
	}
	log.Infof("closed fs watcher on dir %s", dir)
}
