package clone

import (
	"crypto/cipher"
	"crypto/des"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

var blockDES cipher.Block

func init() {
	var err error
	blockDES, err = des.NewCipher(make([]byte, 8))
	if err != nil {
		panic(err)
	}
}

func vhash(shardKey uint64) []byte {
	var keybytes, hashed [8]byte
	binary.BigEndian.PutUint64(keybytes[:], shardKey)
	blockDES.Encrypt(hashed[:], keybytes[:])
	return []byte(hashed[:])
}

//nolint:deadcode,unused
func vunhash(k []byte) (uint64, error) {
	if len(k) != 8 {
		return 0, fmt.Errorf("invalid keyspace id: %v", hex.EncodeToString(k))
	}
	var unhashed [8]byte
	blockDES.Decrypt(unhashed[:], k)
	return binary.BigEndian.Uint64(unhashed[:]), nil
}
