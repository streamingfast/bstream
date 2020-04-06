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

	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

var sourceReconnectDelay = time.Second * 5 // override me on tests

// MultiplexedSource contains a gator based on realtime
type MultiplexedSource struct {
	*shutter.Shutter

	handler Handler

	sourceFactories []SourceFactory
	sources         []Source
	sourcesLock     sync.Mutex
}

func NewMultiplexedSource(sourceFactories []SourceFactory, h Handler) *MultiplexedSource {
	m := &MultiplexedSource{
		handler:         h,
		sourceFactories: sourceFactories,
		sources:         make([]Source, len(sourceFactories)),
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

		zlog.Debug("checking all sources completed, sleeping", zap.Duration("sleep_delay", sourceReconnectDelay))
		time.Sleep(sourceReconnectDelay)
	}
}

func (s *MultiplexedSource) connectSources() {
	s.sourcesLock.Lock()
	defer s.sourcesLock.Unlock()

	zlog.Debug("checking that all sources are properly connected",
		zap.Int("source_count", len(s.sources)),
		zap.Int("source_factory_count", len(s.sourceFactories)),
	)

	if s.IsTerminating() {
		return
	}

	failingSources := 0
	for idx, factory := range s.sourceFactories {
		src := s.sources[idx]

		if src != nil && src.IsTerminating() {
			failingSources++
		}

		if src == nil || src.IsTerminating() {
			shuttingSrcHandler := HandlerFunc(func(blk *Block, obj interface{}) error {
				err := s.handler.ProcessBlock(blk, obj)
				if err != nil {
					zlog.Error("unable to process block, shutting down source")
					s.Shutdown(err)
				}
				return err
			})

			newSrc := factory(shuttingSrcHandler)
			zlog.Info("new source factory created")
			err := s.LockedInit(func() error {
				zlog.Debug("safe running source")
				s.sources[idx] = newSrc
				go newSrc.Run()
				return nil
			})

			if err != nil {
				zlog.Error("safe run", zap.Error(err))
				s.Shutdown(err)
			}
		}
	}

	if failingSources >= len(s.sourceFactories) {
		zlog.Warn("warning, all sources are failing")
	} else if failingSources > 0 {
		zlog.Info("some sources were down", zap.Int("failed_source_count", failingSources))
	} else if failingSources == 0 {
		zlog.Debug("no failing sources detected")
	}
}
