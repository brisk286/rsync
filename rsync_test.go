// Copyright 2012 Julian Gutierrez Oschmann (github.com/julian-gutierrez-o).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

// Unit tests for core package
package rsync

import (
	"fmt"
	"testing"
)
import "io/ioutil"

type filePair struct {
	original string
	modified string
}

func Test_SyncModifiedContent(t *testing.T) {

	//文件队列表
	files := []filePair{filePair{"golang-original.bmp", "golang-modified.bmp"}, filePair{"text-original.txt", "text-modified.txt"}}

	for _, filePair := range files {
		//根据文件名读文件内容
		original, _ := ioutil.ReadFile("test-data/" + filePair.original)
		modified, _ := ioutil.ReadFile("test-data/" + filePair.modified)

		//计算每个块的哈希值
		hashes := CalculateBlockHashes(original)
		opsChannel := make(chan RSyncOp)
		go CalculateDifferences(modified, hashes, opsChannel)

		result := ApplyOps(original, opsChannel, len(modified))

		fmt.Println(result)
		fmt.Println(modified)
		//modified为正确答案
		if string(result) != string(modified) {
			t.Errorf("rsync did not work as expected for %v", filePair)
		}
	}
}

func Test_WeakHash(t *testing.T) {
	content := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	expectedWeak := uint32(10813485)
	expectedA := uint32(45)
	expectedB := uint32(165)
	weak, a, b := weakHash(content)

	assertHash(t, "weak", content, expectedWeak, weak)
	assertHash(t, "a", content, expectedA, a)
	assertHash(t, "b", content, expectedB, b)
}

func Test_Result(t *testing.T) {
	//文件队列表
	files := []filePair{filePair{"text-original.txt", "text-modified.txt"}}

	for _, filePair := range files {
		//根据文件名读文件内容
		original, _ := ioutil.ReadFile("test-data/" + filePair.original)
		modified, _ := ioutil.ReadFile("test-data/" + filePair.modified)

		fmt.Println(original, modified)
		//[115 111 109 101 32 116 101 120 116 13 10]
		//[115 111 109 101 32 101 120 116 114 97 32 116 101 120 116 13 10]

		//计算每个块的哈希值
		//发送方计算哈希块列表，将它发送给接收方
		hashes := CalculateBlockHashes(original)

		opsChannel := make(chan RSyncOp)

		//接收方接收发送方的hash队列，将其用哈希表储存，本地的文件按照块大小与其对比
		//将结果装在通道中，通道中装的是 本地文件按发送方hash队列哈希表的映射
		//DATA:data  BLOCK: index
		go CalculateDifferences(modified, hashes, opsChannel)

		//接收方将通道返回，发送方根据接收方的通道将文件重新组装
		//其中data是文件的修改部分，block以index的形式复原
		result := ApplyOps(original, opsChannel, len(modified))

		fmt.Println(result, modified)
		//fmt.Println()
		//modified为正确答案
		if string(result) != string(modified) {
			t.Errorf("rsync did not work as expected for %v", filePair)
		}
	}
}

func assertHash(t *testing.T, name string, content []byte, expected uint32, found uint32) {
	if found != expected {
		t.Errorf("Incorrent "+name+" hash for %v - Expected %d - Found %d", content, expected, found)
	}
}
