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
	"encoding/hex"

	"github.com/tecbot/gorocksdb"
)

func init() {
	Register("rocksdb", newKVMeta)
}

type rocksdbClient struct {
	*gorocksdb.TransactionDB
}

func newRocksdbClient(addr string) (tkvClient, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCompression(gorocksdb.NoCompression)
	opts.SetWriteBufferSize(10 * 1024 * 1024)
	filter := gorocksdb.NewBloomFilter(10)
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetFilterPolicy(filter)
	opts.SetBlockBasedTableFactory(bbto)
	transactionDBOpts := gorocksdb.NewDefaultTransactionDBOptions()
	transactionDBOpts.SetDefaultLockTimeout(0)
	transactionDBOpts.SetTransactionLockTimeout(0)
	transactionDBOpts.SetMaxNumLocks(64)
	db, err := gorocksdb.OpenTransactionDb(opts, transactionDBOpts, addr)
	return &rocksdbClient{db}, err
}

func (c *rocksdbClient) name() string {
	return "rocksdb"
}

func (c *rocksdbClient) txn(f func(kvTxn) error) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	ro := gorocksdb.NewDefaultReadOptions()
	to := gorocksdb.NewDefaultTransactionOptions()
	to.SetDeadlockDetect(true)
	to.SetDeadlockDetectDepth(50)

	txn := c.TransactionBegin(wo, to, nil)
	defer txn.Destroy()

	tx := &rocksdbTxn{txn: txn, ro: ro}
	if err := f(tx); err != nil {
		txn.Rollback()
		return err
	}
	return txn.Commit()
}

type rocksdbTxn struct {
	txn *gorocksdb.Transaction
	ro  *gorocksdb.ReadOptions
}

func (tx *rocksdbTxn) get(key []byte) []byte {
	value, err := tx.txn.Get(tx.ro, key)
	defer value.Free()
	if err != nil {
		panic(err.Error())
	}
	if !value.Exists() {
		return nil
	}
	logger.Infof("get key: %s, value: %s", hex.Dump(key), hex.Dump(value.Data()))
	result := make([]byte, value.Size())
	copy(result, value.Data())
	logger.Infof("get result: %s", hex.Dump(result))
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
	it := tx.txn.NewIterator(tx.ro)
	defer it.Close()
	it.Seek(prefix)
	var ret [][]byte
	for it.Valid() {
		key := it.Key()
		ret = append(ret, key.Data())
		key.Free()
		if err := it.Err(); err != nil {
			panic(err.Error())
		}
	}
	return ret
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
	logger.Infof("set key: %s, value: %s", hex.Dump(key), hex.Dump(value))
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
		logger.Infof("del: %s", hex.Dump(key))
		if err := tx.txn.Delete(key); err != nil {
			panic(err.Error())
		}
	}
}
