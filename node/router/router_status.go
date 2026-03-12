/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"sync"
)

type RouterState int

const (
	RouterStateInitializing RouterState = iota
	RouterStateRunning
	RouterStateStopping
	RouterStateStopped
	RouterPendingAdmin
)

type RouterStatus struct {
	mu sync.RWMutex

	state                RouterState
	configSequenceNumber uint32
}

func (s *RouterStatus) SetState(state RouterState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state
}

func (s *RouterStatus) SetConfigSequenceNumber(seq uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.configSequenceNumber = seq
}

func (s *RouterStatus) Set(state RouterState, seq uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state
	s.configSequenceNumber = seq
}

func (s *RouterStatus) Get() (RouterState, uint32) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.state, s.configSequenceNumber
}

func (s *RouterStatus) State() RouterState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *RouterStatus) ConfigSequenceNumber() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configSequenceNumber
}

func (s RouterState) String() string {
	switch s {
	case RouterStateInitializing:
		return "initializing"
	case RouterStateRunning:
		return "running"
	case RouterStateStopping:
		return "stopping"
	case RouterStateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

func (s *RouterStatus) StateCompareAndSwapAny(to RouterState, from ...RouterState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, f := range from {
		if s.state == f {
			s.state = to
			return true
		}
	}

	return false
}
