package onboard

import (
	"errors"
	"strings"

	"github.com/treeverse/lakefs/catalog/rocks"
)

// prefixMergeIterator takes inventory iterator and commit iterator and merges them.
// If an entry's prefix is in `prefixes`, it would be taken from invIt.
// No entry from committedIt would be returned if its prefix is in prefixes.
type prefixMergeIterator struct {
	invIt       rocks.EntryIterator
	committedIt rocks.EntryIterator

	err      error
	bothDone bool
}

func newPrefixMergeIterator(iterator rocks.EntryIterator, it rocks.EntryListingIterator, prefixes []string) rocks.EntryIterator {
	return &prefixMergeIterator{
		invIt:       iterator,
		committedIt: newIgnorePrefixIterator(it, prefixes),
		err:         nil,
		bothDone:    false,
	}
}

func (pmi *prefixMergeIterator) Next() bool {
	if pmi.err != nil {
		return false
	}

	switch {
	case pmi.invIt.Value() == nil && pmi.committedIt.Value() == nil:
		pmi.bothDone = true
		return false

	case pmi.invIt.Value() == nil:
		return pmi.advanceIt(pmi.committedIt)

	case pmi.committedIt.Value() == nil:
		return pmi.advanceIt(pmi.invIt)

		// from now on - both iterator has values,
		// so we'll always return true
	case strings.Compare(pmi.committedIt.Value().Path.String(), pmi.invIt.Value().Path.String()) < 0:
		_ = pmi.advanceIt(pmi.committedIt)
		return true

	default:
		_ = pmi.advanceIt(pmi.invIt)
		return true
	}
}

func (pmi *prefixMergeIterator) advanceIt(it rocks.EntryIterator) bool {
	hasNext := it.Next()
	pmi.err = it.Err()
	return hasNext
}

var ErrNotSeekable = errors.New("iterator isn't seekable")

func (pmi *prefixMergeIterator) SeekGE(_ rocks.Path) {
	pmi.err = ErrNotSeekable
}

func (pmi *prefixMergeIterator) Value() *rocks.EntryRecord {
	if pmi.err != nil || pmi.bothDone {
		return nil
	}

	committedVal := pmi.committedIt.Value()
	invVal := pmi.invIt.Value()

	if committedVal == nil {
		return invVal
	}
	if invVal == nil {
		return committedVal
	}

	if strings.Compare(committedVal.Path.String(), invVal.Path.String()) < 0 {
		return committedVal
	}

	return invVal
}

func (pmi *prefixMergeIterator) Err() error {
	return pmi.err
}

func (pmi *prefixMergeIterator) Close() {
	pmi.invIt.Close()
	pmi.err = pmi.invIt.Err()
}

type ignorePrefixIterator struct {
	it       rocks.EntryListingIterator
	prefixes []string

	err   error
	value *rocks.EntryRecord
}

func newIgnorePrefixIterator(it rocks.EntryListingIterator, prefixes []string) rocks.EntryIterator {
	return &ignorePrefixIterator{
		it:       it,
		prefixes: prefixes,
		err:      nil,
		value:    nil,
	}
}

func (ipi *ignorePrefixIterator) Next() bool {
	if ipi.err != nil {
		return false
	}

	for ipi.it.Next() {
		// iterate until finding the matching an entry
		// that doesn't start with one of the prefixes.
		val := ipi.it.Value()
		if !ipi.startsWithPrefix(val) {
			ipi.value = &rocks.EntryRecord{
				Path:  val.Path,
				Entry: val.Entry,
			}
			return true
		}
	}

	// reached the end of the iterator
	ipi.err = ipi.it.Err()
	return false
}

func (ipi *ignorePrefixIterator) startsWithPrefix(val *rocks.EntryListing) bool {
	for _, p := range ipi.prefixes {
		if strings.HasPrefix(val.Path.String(), p) {
			return true
		}
	}

	return false
}

func (ipi *ignorePrefixIterator) SeekGE(_ rocks.Path) {
	ipi.err = ErrNotSeekable
}

func (ipi *ignorePrefixIterator) Value() *rocks.EntryRecord {
	if ipi.err != nil {
		return nil
	}

	return ipi.value
}

func (ipi *ignorePrefixIterator) Err() error {
	return ipi.err
}

func (ipi *ignorePrefixIterator) Close() {
	ipi.it.Close()
	ipi.err = ipi.it.Err()
}
