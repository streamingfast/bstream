// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bstream

import (
	"sync"
	"time"

	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

var sourceReconnectDelay = time.Second * 5 // override me on tests

type MultiplexedSourceOption = func(s *MultiplexedSource)

func MultiplexedSourceWithLogger(logger *zap.Logger) MultiplexedSourceOption {
	return func(s *MultiplexedSource) {
		s.logger = logger
	}
}

// MultiplexedSource contains a gator based on realtime
type MultiplexedSource struct {
	*shutter.Shutter

	handler Handler

	sourceFactories []SourceFactory
	sources         []Source
	sourcesLock     sync.Mutex
	handlerLock     sync.Mutex

	logger *zap.Logger
}

func NewMultiplexedSource(sourceFactories []SourceFactory, h Handler, opts ...MultiplexedSourceOption) *MultiplexedSource {
	m := &MultiplexedSource{
		handler:         h,
		sourceFactories: sourceFactories,
		sources:         make([]Source, len(sourceFactories)),
		logger:          zlog,
	}

	for _, opt := range opts {
		opt(m)
	}

	m.Shutter = shutter.New()
	m.Shutter.OnTerminating(func(_ error) {
		m.sourcesLock.Lock()
		defer m.sourcesLock.Unlock()
		for _, s := range m.sources {
			if s != nil {
				s.Shutdown(nil)
			}
		}
	})
	return m
}

func (s *MultiplexedSource) Run() {
	for {
		if s.IsTerminating() {
			return
		}

		s.connectSources()

		time.Sleep(sourceReconnectDelay)
	}
}

func (s *MultiplexedSource) SetLogger(logger *zap.Logger) {
	s.logger = logger
}

func (s *MultiplexedSource) connectSources() {
	s.sourcesLock.Lock()
	defer s.sourcesLock.Unlock()

	if s.IsTerminating() {
		return
	}

	failingSources := 0
	for idx, factory := range s.sourceFactories {
		src := s.sources[idx]

		if src == nil || src.IsTerminating() {
			shuttingSrcHandler := HandlerFunc(func(blk *Block, obj interface{}) error {
				s.handlerLock.Lock()
				err := s.handler.ProcessBlock(blk, obj)
				s.handlerLock.Unlock()
				if err != nil {
					s.logger.Error("unable to process block, shutting down source")
					s.Shutdown(err)
				}
				return err
			})

			newSrc := factory(shuttingSrcHandler)
			s.logger.Info("new source factory created")
			err := s.LockedInit(func() error {
				s.logger.Debug("safe running source")
				s.sources[idx] = newSrc
				go newSrc.Run()
				return nil
			})

			if err != nil {
				s.logger.Error("safe run", zap.Error(err))
				s.Shutdown(err)
			}
		}
	}

	if failingSources >= len(s.sourceFactories) {
		s.logger.Warn("warning, all sources are failing",
			zap.Int("source_count", len(s.sources)),
			zap.Int("source_factory_count", len(s.sourceFactories)),
		)
	} else if failingSources > 0 {
		s.logger.Info("some sources were down",
			zap.Int("failed_source_count", failingSources),
			zap.Int("source_count", len(s.sources)),
			zap.Int("source_factory_count", len(s.sourceFactories)),
		)
	}
}
