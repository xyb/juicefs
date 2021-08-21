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

	"github.com/linxGnu/grocksdb"
)

func init() {
	Register("rocksdb", newKVMeta)
}

type rocksdbClient struct {
	*grocksdb.OptimisticTransactionDB
}

func newRocksdbClient(addr string) (tkvClient, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCompression(grocksdb.NoCompression)
	opts.SetWriteBufferSize(10 * 1024 * 1024)
	filter := grocksdb.NewBloomFilter(10)
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetFilterPolicy(filter)
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 20))
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetPrefixExtractor(grocksdb.NewFixedPrefixTransform(9)) // Aiiiiiiii
	db, err := grocksdb.OpenOptimisticTransactionDb(opts, addr)
	return &rocksdbClient{db}, err
}

func (c *rocksdbClient) name() string {
	return "rocksdb"
}

func (c *rocksdbClient) txn(f func(kvTxn) error) error {
	wo := grocksdb.NewDefaultWriteOptions()
	ro := grocksdb.NewDefaultReadOptions()
	to := grocksdb.NewDefaultOptimisticTransactionOptions()

	txn := c.TransactionBegin(wo, to, nil)
	defer txn.Destroy()

	tx := &rocksdbTxn{txn: txn, ro: ro}
	if err := f(tx); err != nil {
		//logger.Infof("txn error: %s", err.Error())
		txn.Rollback()
		return err
	}
	return txn.Commit()
}

type rocksdbTxn struct {
	txn *grocksdb.Transaction
	ro  *grocksdb.ReadOptions
}

func (tx *rocksdbTxn) get(key []byte) []byte {
	value, err := tx.txn.Get(tx.ro, key)
	defer value.Free()
	if !value.Exists() {
		return nil
	}
	if err != nil {
		panic(err.Error())
	}
	//logger.Infof("get key: %s, value: %s", hex.Dump(key), hex.Dump(value.Data()))
	result := make([]byte, value.Size())
	copy(result, value.Data())
	//logger.Infof("get result: %s", hex.Dump(result))
	return result
}

func (tx *rocksdbTxn) gets(keys ...[]byte) [][]byte {
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = tx.get(key)
	}
	return values
}

func (tx *rocksdbTxn) scanRange0(begin, end []byte, filter func(k, v []byte) bool) map[string][]byte {
	it := tx.txn.NewIterator(tx.ro)
	defer it.Close()
	var ret = make(map[string][]byte)
	for it.Seek(begin); it.Valid(); it.Next() {
		key := it.Key()
		if bytes.Compare(key.Data(), end) >= 0 {
			break
		}
		value := it.Value()
		if filter == nil || filter(key.Data(), value.Data()) {
			k := make([]byte, key.Size())
			copy(k, key.Data())
			v := make([]byte, value.Size())
			copy(v, value.Data())
			ret[string(k)] = v
		}
		key.Free()
		value.Free()
		if err := it.Err(); err != nil {
			panic(err.Error())
		}
	}
	return ret
}

func (tx *rocksdbTxn) scanRange(begin, end []byte) map[string][]byte {
	return tx.scanRange0(begin, end, nil)
}

func (tx *rocksdbTxn) nextKey(key []byte) []byte {
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

func (tx *rocksdbTxn) scanKeys(prefix []byte) [][]byte {
	var keys [][]byte
	it := tx.txn.NewIterator(tx.ro)
	defer it.Close()
	for it.Seek(prefix); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key.Data(), prefix) {
			break
		}
		k := make([]byte, key.Size())
		copy(k, key.Data())
		keys = append(keys, k)
		key.Free()
		if err := it.Err(); err != nil {
			panic(err.Error())
		}
	}
	return keys
}

func (tx *rocksdbTxn) scanValues(prefix []byte, filter func(k, v []byte) bool) map[string][]byte {
	return tx.scanRange0(prefix, tx.nextKey(prefix), filter)
}

func (tx *rocksdbTxn) exist(prefix []byte) bool {
	it := tx.txn.NewIterator(tx.ro)
	defer it.Close()
	it.Seek(prefix)
	return it.Valid()
}

func (tx *rocksdbTxn) set(key, value []byte) {
	//logger.Infof("set key: %s, value: %s", hex.Dump(key), hex.Dump(value))
	if err := tx.txn.Put(key, value); err != nil {
		panic(err.Error())
	}
}

func (tx *rocksdbTxn) append(key []byte, value []byte) []byte {
	new := append(tx.get(key), value...)
	tx.set(key, new)
	return new
}

func (tx *rocksdbTxn) incrBy(key []byte, value int64) int64 {
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

func (tx *rocksdbTxn) dels(keys ...[]byte) {
	for _, key := range keys {
		//logger.Infof("del: %s", hex.Dump(key))
		if err := tx.txn.Delete(key); err != nil {
			panic(err.Error())
		}
		//logger.Infof("del OK: %s", hex.Dump(key))
	}
}
