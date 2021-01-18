package migrate

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/mvcc"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/db"
)

type iteratorState int

const (
	iteratorStateInit iteratorState = iota
	iteratorStateQuerying
	iteratorStateDone
	iteratorStateClosed
)

type Iterator struct {
	state     iteratorState
	ctx       context.Context
	db        db.Database
	branchID  int64
	commitID  int64
	buf       []*rocks.EntryRecord
	err       error
	offset    string
	fetchSize int
	value     *rocks.EntryRecord
}

var ErrIteratorClosed = errors.New("iterator closed")

// NewIterator accepts mvcc branch/commit, read entries as EntryCatalog entries
func NewIterator(ctx context.Context, db db.Database, branchID int64, commitID int64, fetchSize int) *Iterator {
	return &Iterator{
		ctx:       ctx,
		db:        db,
		branchID:  branchID,
		commitID:  commitID,
		buf:       make([]*rocks.EntryRecord, 0, fetchSize),
		fetchSize: fetchSize,
	}
}

func (it *Iterator) Next() bool {
	if it.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	if it.err != nil {
		return false
	}

	it.fetch()

	// stage a value and increment offset
	if len(it.buf) == 0 {
		return false
	}
	it.value = it.buf[0]
	it.offset = string(it.value.Path)
	if len(it.buf) > 1 {
		it.buf = it.buf[1:]
	} else {
		it.buf = it.buf[:0]
	}
	return true
}

func (it *Iterator) SeekGE(id rocks.Path) {
	if it.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	it.state = iteratorStateInit
	it.offset = id.String()
	it.buf = it.buf[:0]
	it.value = nil
	it.err = nil
}

func (it *Iterator) Value() *rocks.EntryRecord {
	if it.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	if it.err != nil {
		return nil
	}
	return it.value
}

func (it *Iterator) Err() error {
	if it.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	return it.err
}

func (it *Iterator) Close() {
	if it.state == iteratorStateClosed {
		return
	}
	it.buf = nil
	it.state = iteratorStateClosed
	it.err = ErrIteratorClosed
}

func (it *Iterator) fetch() {
	if it.state == iteratorStateDone {
		return
	}
	if len(it.buf) > 0 {
		return
	}
	if it.state == iteratorStateInit {
		it.state = iteratorStateQuerying
	}

	var res interface{}
	res, it.err = it.db.Transact(func(tx db.Tx) (interface{}, error) {
		return mvcc.ListEntriesTx(tx, it.branchID, mvcc.CommitID(it.commitID), "", it.offset, it.fetchSize)
	}, db.WithContext(it.ctx), db.ReadOnly())
	if it.err != nil {
		return
	}
	entries := res.([]*catalog.Entry)
	for _, entry := range entries {
		rec := &rocks.EntryRecord{
			Path:  rocks.Path(entry.Path),
			Entry: rocks.EntryFromCatalogEntry(*entry),
		}
		it.buf = append(it.buf, rec)
	}
	if len(it.buf) < it.fetchSize {
		it.state = iteratorStateDone
	} else if len(it.buf) > 0 {
		it.offset = it.buf[len(it.buf)-1].Path.String()
	}
}

type emptyIterator struct{}

func (e *emptyIterator) Next() bool {
	return false
}

func (e *emptyIterator) SeekGE(rocks.Path) {
}

func (e *emptyIterator) Value() *rocks.EntryRecord {
	return nil
}

func (e *emptyIterator) Err() error {
	return nil
}

func (e *emptyIterator) Close() {
}

func newEmptyIterator() rocks.EntryIterator {
	return &emptyIterator{}
}
