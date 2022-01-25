package embed

import "github.com/codenotary/immudb/pkg/state"

type EmbedOption func(embed *Embed)

func Dir(dir string) EmbedOption {
	return func(args *Embed) {
		args.dir = dir
	}
}

func DatabaseName(dbName string) EmbedOption {
	return func(args *Embed) {
		args.databaseName = dbName
	}
}

func StateService(stateService state.StateService) EmbedOption {
	return func(args *Embed) {
		args.stateService = stateService
	}
}
