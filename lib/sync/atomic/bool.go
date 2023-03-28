package atomic

import "sync/atomic"

type Boolen uint32

func (b *Boolen) Get() bool {
	return atomic.LoadUint32((*uint32)(b)) != 0
}

func (b *Boolen) Set(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(b), 1)
	} else {
		atomic.StoreUint32((*uint32)(b), 0)
	}
}
