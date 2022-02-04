// Copyright 2012 Julian Gutierrez Oschmann (github.com/julian-gutierrez-o).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

// Package rsync A Golang implementation of the rsync algorithm.
// This package contains the algorithm for both client and server side.
package rsync

import (
	"crypto/md5"
)

const (
	// BlockSize 默认块大小
	//BlockSize = 1024 * 644
	BlockSize = 2
	// M 65536 弱哈希算法取模
	M = 1 << 16
)

// BlockHash hash块结构
type BlockHash struct {
	//哈希块下标
	index int
	//强哈希值
	strongHash []byte
	//弱哈希值
	weakHash uint32
}

// There are two kind of operations: BLOCK and DATA.
// If a block match is found on the server, a BLOCK operation is sent over the channel along with the block index.
// Modified data between two block matches is sent like a DATA operation.
//常量
const (
	// BLOCK 整块数据
	BLOCK = iota
	// DATA 单独修改数据
	DATA
)

// RSyncOp An rsync operation (typically to be sent across the network). It can be either a block of raw data or a block index.
//rsync数据体
type RSyncOp struct {
	//操作类型
	opCode int
	//如果是DATA 那么保存数据
	data []byte
	//如果是BLOCK 保存块下标
	blockIndex int
}

// CalculateBlockHashes Returns weak and strong hashes for a given slice.
//计算每个块的哈希值
//参数：全部数据内容
//返回：每个块组成的列表
func CalculateBlockHashes(content []byte) []BlockHash {
	blockHashes := make([]BlockHash, getBlocksNumber(content))
	for i := range blockHashes {
		initialByte := i * BlockSize
		endingByte := min((i+1)*BlockSize, len(content))
		// 确认每个块的定位
		block := content[initialByte:endingByte]
		//计算此块的弱hash
		weak, _, _ := weakHash(block)
		//保存到块哈希数组中
		blockHashes[i] = BlockHash{
			index:      i,
			strongHash: strongHash(block),
			weakHash:   weak,
		}
	}
	return blockHashes
}

// Returns the number of blocks for a given slice of content.
//计算文件需要块的数量
func getBlocksNumber(content []byte) int {
	blockNumber := len(content) / BlockSize
	if len(content)%BlockSize != 0 {
		blockNumber += 1
	}
	return blockNumber
}

// ApplyOps Applies operations from the channel to the original content.
// Returns the modified content.
//根据通道接收到的信息，将数据组装发送
//参数：文件内容，数据操作体 通道， 本地文件大小
//返回:组装后的数据
func ApplyOps(content []byte, ops chan RSyncOp, fileSize int) []byte {
	result := make([]byte, fileSize)

	//遍历通道接收到的数据
	var offset int
	for op := range ops {
		switch op.opCode {
		case BLOCK:
			//copy：目标文件，源文件
			copy(result[offset:offset+BlockSize], content[op.blockIndex*BlockSize:op.blockIndex*BlockSize+BlockSize])
			offset += BlockSize
		//DATA是不定长的
		case DATA:
			copy(result[offset:], op.data)
			offset += len(op.data)
		}
	}
	return result
}

// CalculateDifferences Computes all the operations needed to recreate content.
// All these operations are sent through a channel of RSyncOp.
//计算不同
//不返回，将处理的数据放入通道
//参数：本地文件内容， 传送过来的块哈希数组， 空操作通道
func CalculateDifferences(content []byte, hashes []BlockHash, opsChannel chan RSyncOp) {

	//构建一个哈希map，<下标，哈希块列表>？ 链表结构？
	hashesMap := make(map[uint32][]BlockHash)
	defer close(opsChannel)

	//遍历每个哈希块数组
	for _, h := range hashes {
		key := h.weakHash
		//用弱hash做key，值为哈希块
		//数组+链表！！todo：Test
		hashesMap[key] = append(hashesMap[key], h)
	}

	//移动下标  前一个匹配块的尾部
	var offset, previousMatch int
	//弱hash 3个数值
	var aweak, bweak, weak uint32
	//标记
	var dirty, isRolling bool

	for offset < len(content) {
		//一个块的尾部
		endingByte := min(offset+BlockSize, len(content)-1)
		block := content[offset:endingByte]
		//如果不用rolling
		if !isRolling {
			//弱hash的三个值
			weak, aweak, bweak = weakHash(block)
			//如果没找到对应的块  下一次进行rolling
			isRolling = true
			//如果一直找不到会一直rolling，直到找个能对应的块，两个能对应的块之间都是DATA
		} else {
			//rolling操作 计算下一个step 1 的hash值
			aweak = (aweak - uint32(content[offset-1]) + uint32(content[endingByte-1])) % M
			bweak = (bweak - (uint32(endingByte-offset) * uint32(content[offset-1])) + aweak) % M
			weak = aweak + (1 << 16 * bweak)
		}
		//如果在hashmap中找到了弱hash对应的块， 弱hash找用hashmap
		if l := hashesMap[weak]; l != nil {
			//强hash找用遍历
			blockFound, blockHash := searchStrongHash(l, strongHash(block))
			//如果从hash块队列中找到了强hash块
			if blockFound {
				//如果是DATA
				if dirty {
					//将一个数组操作体放入操作管道中
					opsChannel <- RSyncOp{opCode: DATA, data: content[previousMatch:offset]}
					dirty = false
				}
				//将一个数组操作体放入操作管道中
				opsChannel <- RSyncOp{opCode: BLOCK, blockIndex: blockHash.index}
				previousMatch = endingByte
				// 找到了就不用rolling
				isRolling = false
				offset += BlockSize
				continue
			}
		}
		//如果找不到弱hash对应的块 将下一轮搜索的块标记为DATA
		dirty = true
		//rolling
		offset++
	}

	//如果最后一个块不对应,那么把所有DATA放入
	if dirty {
		opsChannel <- RSyncOp{opCode: DATA, data: content[previousMatch:]}
	}
}

// Searches for a given strong hash among all strong hashes in this bucket.
//从hash块队列中遍历每个块的强hash值  一一比对
func searchStrongHash(l []BlockHash, hashValue []byte) (bool, *BlockHash) {
	for _, blockHash := range l {
		if string(blockHash.strongHash) == string(hashValue) {
			return true, &blockHash
		}
	}
	return false, nil
}

// Returns a strong hash for a given block of data
func strongHash(v []byte) []byte {
	h := md5.New()
	h.Write(v)
	return h.Sum(nil)
}

// Returns a weak hash for a given block of data.
//弱hash
func weakHash(v []byte) (uint32, uint32, uint32) {
	var a, b uint32
	for i := range v {
		a += uint32(v[i])
		b += (uint32(len(v)-1) - uint32(i) + 1) * uint32(v[i])
	}
	return (a % M) + (1 << 16 * (b % M)), a % M, b % M
}

// Returns the smaller of a or b.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
