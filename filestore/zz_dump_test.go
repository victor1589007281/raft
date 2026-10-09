package filestore

// zz_dump_test.go —— 一次性诊断工具(非回归用例): 用生产解析器逐条吐出 raft.log
// 的 meta 记录, 用来判定「索引空洞 / 尾部截断」到底是【崩溃撕尾】还是
// 【代码主动 truncate】。跑法:
//   RAFTLOG=/tmp/fsp-evidence-105/s0-raft.log go test ./filestore/ -run TestDumpRaftLog -v

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func TestDumpRaftLog(t *testing.T) {
	path := os.Getenv("RAFTLOG")
	if path == "" {
		t.Skip("RAFTLOG not set")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("file=%s size=%d\n", path, len(raw))
	// 文件头
	hdr := raw[:fileHeaderLen]
	fmt.Printf("header magic=%x flags=%x\n", hdr[0:4], binary.LittleEndian.Uint32(hdr[4:8]))

	lastIdx := uint64(0)
	count := 0
	gaps := 0
	start := uint64(0)
	good, stats, err := scanRecords(raw, fileHeaderLen, true, func(off int64, body []byte) error {
		h, tail, derr := decodeMetaRecord(body)
		if derr != nil {
			return derr
		}
		if count == 0 {
			start = h.index
		} else if h.index != lastIdx+1 {
			gaps++
			fmt.Printf("  ★GAP: %d -> %d (缺 %d 条)\n", lastIdx, h.index, h.index-lastIdx-1)
		}
		dataLen, extLen, lsnN := 0, 0, 0
		if tail != nil {
			dataLen, extLen, lsnN = len(tail.Data), len(tail.Extensions), len(tail.LSNs)
		}
		fmt.Printf("off=%-9d idx=%-7d term=%-3d type=%d flags=0x%02x base=%-5d lsn=%-9d tail:data=%d ext=%d lsns=%d\n",
			off, h.index, h.term, h.logType, h.flags, h.base, h.lsn, dataLen, extLen, lsnN)
		lastIdx = h.index
		count++
		return nil
	})
	fmt.Printf("--- scan: goodEnd=%d (fileEnd=%d) err=%v holes=%d holesBytes=%d\n", good, len(raw), err, stats.holes, stats.holesBytes)
	fmt.Printf("--- records=%d first=%d last=%d gaps=%d\n", count, start, lastIdx, gaps)

	// 尾部形态: goodEnd 之后到 EOF 是否全零?
	allZero := true
	for i := good; i < int64(len(raw)); i++ {
		if raw[i] != 0 {
			allZero = false
			fmt.Printf("--- 尾部首个非零字节 @%d\n", i)
			break
		}
	}
	fmt.Printf("--- goodEnd=%d 512对齐=%v 之后全零=%v (剩余 %d B)\n", good, good%512 == 0, allZero, int64(len(raw))-good)
}
