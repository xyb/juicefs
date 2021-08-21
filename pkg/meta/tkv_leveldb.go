// +build !fdb

/*
 * JuiceFS, Copyright (C) 2021 Juicedata, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package meta

import (
	"bytes"
	"sync"

	"github.com/jmhodges/levigo"
)

func init() {
	Register("leveldb", newKVMeta)
}

func newLeveldbClient(addr string) (tkvClient, error) {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(4 << 20))
	opts.SetCreateIfMissing(true)
	ldb, err := levigo.Open(addr, opts)
	return &leveldbClient{ldb: ldb}, err
}

type leveldbClient struct {
	sync.Mutex
	ldb *levigo.DB
}

type ldbTxn struct {
	client *leveldbClient
	ro     *levigo.ReadOptions
	wo     *levigo.WriteOptions
	wb     *levigo.WriteBatch
}

func (tx *ldbTxn) get(key []byte) []byte {
	value, err := tx.client.ldb.Get(tx.ro, key)
	if err != nil {
		panic(err)
	}
	if len(value) == 0 {
		return nil
	}
	return value
}

func (tx *ldbTxn) gets(keys ...[]byte) [][]byte {
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = tx.get(key)
	}
	return values
}

func (tx *ldbTxn) scanRange0(begin, end []byte, filter func(k, v []byte) bool) map[string][]byte {
	ro := levigo.NewReadOptions()
	ro.SetFillCache(false)
	it := tx.client.ldb.NewIterator(ro)
	defer it.Close()
	ret := make(map[string][]byte)
	for it.Seek(begin); it.Valid(); it.Next() {
		key := it.Key()
		if bytes.Compare(key, end) >= 0 {
			break
		}
		value := it.Value()
		if filter == nil || filter(key, value) {
			ret[string(key)] = value
		}
	}
	if err := it.GetError(); err != nil {
		panic(err)
	}
	return ret
}

func (tx *ldbTxn) scanRange(begin, end []byte) map[string][]byte {
	return tx.scanRange0(begin, end, nil)
}

func (tx *ldbTxn) nextKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	next := make([]byte, len(key))
	copy(next, key)
	p := len(next) - 1
	for {
		next[p]++
		if next[p] != 0 {
			break
		}
		p--
		if p < 0 {
			panic("can't scan keys for 0xFF")
		}
	}
	return next
}

func (tx *ldbTxn) scanKeys(prefix []byte) [][]byte {
	var keys [][]byte
	for k := range tx.scanValues(prefix, nil) {
		keys = append(keys, []byte(k))
	}
	return keys
}

func (tx *ldbTxn) scanValues(prefix []byte, filter func(k, v []byte) bool) map[string][]byte {
	return tx.scanRange0(prefix, tx.nextKey(prefix), filter)
}

func (tx *ldbTxn) exist(prefix []byte) bool {
	ro := levigo.NewReadOptions()
	ro.SetFillCache(false)
	it := tx.client.ldb.NewIterator(ro)
	defer it.Close()
	it.Seek(prefix)
	if err := it.GetError(); err != nil {
		panic(err)
	}
	return it.Valid()
}

func (tx *ldbTxn) set(key, value []byte) {
	tx.wb.Put(key, value)
}

func (tx *ldbTxn) append(key []byte, value []byte) []byte {
	new := append(tx.get(key), value...)
	tx.set(key, new)
	return new
}

func (tx *ldbTxn) incrBy(key []byte, value int64) int64 {
	var new int64
	buf := tx.get(key)
	if len(buf) > 0 {
		new = parseCounter(buf)
	}
	if value != 0 {
		new += value
		tx.set(key, packCounter(new))
	}
	return new
}

func (tx *ldbTxn) dels(keys ...[]byte) {
	for _, key := range keys {
		tx.wb.Delete(key)
	}
}

func (c *leveldbClient) name() string {
	return "leveldb"
}

func (c *leveldbClient) txn(f func(kvTxn) error) error {
	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()
	snap := c.ldb.NewSnapshot()
	defer c.ldb.ReleaseSnapshot(snap)
	ro.SetSnapshot(snap)
	wb := levigo.NewWriteBatch()
	defer wb.Close()

	tx := &ldbTxn{
		client: c,
		ro:     ro,
		wo:     wo,
		wb:     wb,
	}

	//c.Lock()
	//defer c.Unlock()
	if err := f(tx); err != nil {
		return err
	}

	if err := c.ldb.Write(tx.wo, tx.wb); err != nil {
		panic(err)
	}

	return nil
}
