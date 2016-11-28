package tupleSpace

import (
	//"code.google.com/p/go-uuid/uuid"
	"github.com/pborman/uuid"
	"sync"
)

type TupleSpace interface {
	Write(tuple Tuple)
	Read(tuple Tuple) chan Tuple
	Take(tuple Tuple) chan Tuple
	Watch(tuple Tuple, receiver chan Tuple) uuid.UUID
	Cancel(id uuid.UUID) bool
	Len() int
}

type tupleManager struct {
	tuples []Tuple
	mutex  *sync.RWMutex
}

func (tM *tupleManager) Read(tuple Tuple, receiver chan Tuple) {
	tM.mutex.RLock()
	defer tM.mutex.RUnlock()

	for i := len(tM.tuples) - 1; 0 <= i; i-- {
		t := tM.tuples[i]
		if t.Match(tuple) && !t.IsExpired() {
			receiver <- t
			return
		}
	}
	receiver <- New(0, `0`)
	return
}

func (tM *tupleManager) Take(tuple Tuple, receiver chan Tuple) {
	tM.mutex.Lock()
	defer tM.mutex.Unlock()

	for i := len(tM.tuples) - 1; 0 <= i; i-- {
		t := tM.tuples[i]
		if t.Match(tuple) && !t.IsExpired() {
			if i == 0 {
				tM.tuples = make([]Tuple, 0)
			} else {
				tM.tuples = tM.tuples[:i]
			}

			receiver <- t
			return
		}
	}
	receiver <- New(0, `0`)
	return
}

func (tM *tupleManager) Write(tuple Tuple) {
	tM.mutex.Lock()
	defer tM.mutex.Unlock()

	tM.tuples = append(tM.tuples, tuple)
}

func (tM *tupleManager) Len() int {
	tM.mutex.RLock()
	defer tM.mutex.RUnlock()
	return len(tM.tuples)
}

type callback struct {
	Tuple    Tuple
	Receiver chan Tuple
}

type watchersManager struct {
	watchers map[string]*callback
	mutex    *sync.RWMutex
}

func (wM *watchersManager) Register(tuple Tuple, receiver chan Tuple) uuid.UUID {
	wM.mutex.Lock()
	defer wM.mutex.Unlock()

	id := uuid.NewRandom()

	wM.watchers[id.String()] = &callback{
		Tuple:    tuple,
		Receiver: receiver,
	}

	return id
}

func (wM *watchersManager) Cancel(id uuid.UUID) bool {
	wM.mutex.Lock()
	defer wM.mutex.Unlock()

	key := id.String()

	if _, ok := wM.watchers[key]; ok {
		delete(wM.watchers, key)
		return true
	}

	return false
}

func (wM *watchersManager) Match(t Tuple) chan Tuple {
	wM.mutex.RLock()
	defer wM.mutex.RUnlock()

	for _, callback := range wM.watchers {
		if t.Match(callback.Tuple) && !t.IsExpired() {
			return callback.Receiver
		}
	}

	return nil
}

type tupleSpace struct {
	tuples   *tupleManager
	watchers *watchersManager
}

func NewSpace() TupleSpace {
	return &tupleSpace{
		tuples: &tupleManager{
			tuples: make([]Tuple, 0),
			mutex:  new(sync.RWMutex),
		},
		watchers: &watchersManager{
			watchers: make(map[string]*callback),
			mutex:    new(sync.RWMutex),
		},
	}
}

func (s *tupleSpace) Read(tuple Tuple) chan Tuple {
	found := make(chan Tuple)

	go func() { s.tuples.Read(tuple, found) }()

	return found
}

func (s *tupleSpace) Take(tuple Tuple) chan Tuple {
	found := make(chan Tuple)

	go func() { s.tuples.Take(tuple, found) }()

	return found
}

func (s *tupleSpace) Len() int {
	return s.tuples.Len()
}

func (s *tupleSpace) Watch(tuple Tuple, receiver chan Tuple) uuid.UUID {
	return s.watchers.Register(tuple, receiver)
}

func (s *tupleSpace) Cancel(id uuid.UUID) bool {
	return s.watchers.Cancel(id)
}

func (s *tupleSpace) Write(tuple Tuple) {
	recv := s.watchers.Match(tuple)

	if recv != nil {
		recv <- tuple
	} else {
		s.tuples.Write(tuple)
	}
}