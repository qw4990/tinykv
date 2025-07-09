package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	writeIter engine_util.DBIterator
	txn       *MvccTxn
	finished  bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	scanner := &Scanner{
		writeIter: writeIter,
		txn:       txn,
		finished:  false,
	}
	seekKey := EncodeKey(startKey, ^uint64(0))
	scanner.writeIter.Seek(seekKey)
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.writeIter != nil {
		scan.writeIter.Close()
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.finished {
		return nil, nil, nil
	}

	for scan.writeIter.Valid() {
		item := scan.writeIter.Item()
		writeKey := item.Key()
		userKey := DecodeUserKey(writeKey)
		commitTs := decodeTimestamp(writeKey)

		if commitTs <= scan.txn.StartTS {
			writeValue, err := item.Value()
			if err != nil {
				return nil, nil, err
			}

			write, err := ParseWrite(writeValue)
			if err != nil {
				return nil, nil, err
			}

			if write.Kind == WriteKindPut {
				valueKey := EncodeKey(userKey, write.StartTS)
				value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, valueKey)
				if err != nil {
					return nil, nil, err
				}
				scan.skipToNextUserKey(userKey)
				return userKey, value, nil
			} else {
				scan.skipToNextUserKey(userKey)
				continue
			}
		}
		scan.writeIter.Next()
	}
	scan.finished = true
	return nil, nil, nil
}

func (scan *Scanner) skipToNextUserKey(currentUserKey []byte) {
	for scan.writeIter.Valid() {
		scan.writeIter.Next()
		if !scan.writeIter.Valid() {
			break
		}

		item := scan.writeIter.Item()
		writeKey := item.Key()
		userKey := DecodeUserKey(writeKey)

		if !bytes.Equal(userKey, currentUserKey) {
			break
		}
	}
}
