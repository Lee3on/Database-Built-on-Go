package byodb01

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
)

func SaveData1(path string, data []byte) error {
	// Open a file for writing. The file is created if it doesn't exist
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close() // Make sure to close the file when you're done

	_, err = fp.Write(data)
	return err
}

func randomInt() int {
	var buf [8]byte
	rand.Read(buf[:])
	return int(binary.LittleEndian.Uint64(buf[:]))
}

// Atomic Renaming
func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		os.Remove(tmp)
		return err
	}

	return os.Rename(tmp, path)
}

// Atomic Renaming with fsync
func SaveData3(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		os.Remove(tmp)
		return err
	}

	// Flush the data to the disk before renaming it
	err = fp.Sync() // fsync
	if err != nil {
		os.Remove(tmp)
		return err
	}

	return os.Rename(tmp, path)
}
