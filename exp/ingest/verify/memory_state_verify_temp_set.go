package verify

type MemoryStateVerifyTempSet struct {
	m map[string]bool
}

// Open initialize internals data structure.
func (s *MemoryStateVerifyTempSet) Open() error {
	s.m = make(map[string]bool)
	return nil
}

// Add adds a key to TempSet.
func (s *MemoryStateVerifyTempSet) Add(key string) error {
	_, ok := s.m[key]
	if ok {
		return errKeyAlreadyExists
	}

	s.m[key] = true
	return nil
}

func (s *MemoryStateVerifyTempSet) Remove(key string) error {
	_, ok := s.m[key]
	if !ok {
		return errKeyNotFound
	}

	delete(s.m, key)
	return nil
}

func (s *MemoryStateVerifyTempSet) IsEmpty() (bool, error) {
	return len(s.m) == 0, nil
}

// Close dereferences internal data structure.
func (s *MemoryStateVerifyTempSet) Close() error {
	s.m = nil
	return nil
}
