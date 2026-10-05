package calling

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// NativeBindingHash joins diagnostics using private session and call identities.
// The contract is SHA-256(domain || uint64-BE session byte length || UTF-8 session
// || uint64-BE call byte length || UTF-8 call), encoded as lowercase hex.
func NativeBindingHash(session, callID string) string {
	data := []byte("gows-native-call-v1\x00")
	data = binary.BigEndian.AppendUint64(data, uint64(len(session)))
	data = append(data, session...)
	data = binary.BigEndian.AppendUint64(data, uint64(len(callID)))
	data = append(data, callID...)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
