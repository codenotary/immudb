package server

import (
	"strconv"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/fsnotify/fsnotify"
)

func setUpWatcher(s *store.Store, dbDir string, pid int, log logger.Logger) (*fsnotify.Watcher, error) {
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

				// if event.Op&fsnotify.Write == fsnotify.Write {
				tamperings, err := store.GetTamperings(dbDir, strconv.Itoa(pid), event.Name)
				if err != nil {
					log.Errorf("error detecting tampering on %s", event.Name)
				}
				if len(tamperings) > 0 {
					s.AppendTamperings(tamperings...)
					log.Warningf("possible tampering detected on %s:\n%+v", event.Name, tamperings)
				}
				// }
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
