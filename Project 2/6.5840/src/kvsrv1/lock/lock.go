package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const KEY_ID_LENGTH = 8
const FREE = "free"
const BACKOFF_TIME = 100

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key   string
	value string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:    ck,
		key:   l,
		value: kvtest.RandValue(KEY_ID_LENGTH),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.key)

		switch {
		case err == rpc.ErrNoKey: // First owner
			if lk.ck.Put(lk.key, lk.value, 0) == rpc.OK {
				return
			}
		case value == FREE:
			if lk.ck.Put(lk.key, lk.value, version) == rpc.OK {
				return
			}
		}

		time.Sleep(BACKOFF_TIME * time.Microsecond)
	}
}

func (lk *Lock) Release() {
	value, version, err := lk.ck.Get(lk.key)
	if err != rpc.OK {
		panic("Tried to release a non-existing lock")
	}
	if value != lk.value {
		return
	}

	for {
		if lk.ck.Put(lk.key, FREE, version) == rpc.OK {
			break
		}
	}
}
