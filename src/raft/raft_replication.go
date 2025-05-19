package raft

import (
	"fmt"
	"sort"
)

type LogEntry struct {
	Term         int
	CommandValid bool // 设置是否实施
	Command      interface{}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// 一起唯一确认日志
	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	LeaderCommit int // 更新follower's commit index
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("leader-%d, T%d, prev:[%d]T%d, (%d, %d], commit indedx: %d", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConfilictIndex int
	ConfilictTerm  int
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, success: %v, conflict term: [%d]T[%d]", reply.Term, reply.Success, reply.ConfilictIndex, reply.ConfilictTerm)
}

// peer's callback
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, appended, args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	} else {
		rf.becomeFollowerLocked(args.Term)
	}

	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, follower confict: [%d]T%d", args.LeaderId, reply.ConfilictIndex, reply.ConfilictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, follower log=%v", args.LeaderId, rf.log.String())
		}
	}()

	// 如果prevlog未匹配上
	if args.PrevLogIndex >= rf.log.size() {
		reply.ConfilictIndex = rf.log.size()
		reply.ConfilictTerm = InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, len:%d <=prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}

	// 说明从这里（或前面）开始日志丢失
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log, pre log not match, {%d}: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}

	// 添加leader's log entries 到本地
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLeader, "follower accept logs: (%d %d)", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 处理leader commit
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 返回一半peer已匹配上的日志索引
func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexes))
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]

}

func (rf *Raft) startReplication(term int) bool {

	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d Lost or crashed", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, append, reply=%v", peer, reply.String())

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, context lost, T%d:Leader->T%d:%d", peer, term, rf.currentTerm, rf.role)
			return
		}

		// 处理回复

		// 失败 回退
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]

			if reply.ConfilictTerm == InvalidTerm { // 说明follower日志过短
				rf.nextIndex[peer] = reply.ConfilictIndex
			} else {
				firstIndex := rf.log.firstFor(reply.ConfilictTerm)

				if firstIndex != InvalidIndex { // leader有该term
					rf.nextIndex[peer] = firstIndex
				} else {
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}

			// 避免乱序
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}

			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, not matched at prev [%d]T%d, try next prev=[%d]T[%d]",
				peer, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[peer]-1, rf.log.at(rf.nextIndex[peer]-1))
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, leader log=%v", peer, rf.log.String())
			return
		}

		// 成功

		// 更新匹配和next索引

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) // 不能使用自己的变量，因为可能会更新 ！！！
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// 更新提交索引
		majorityMathced := rf.getMajorityIndexLocked()
		if majorityMathced > rf.commitIndex && rf.log.at(majorityMathced).Term == rf.currentTerm { // figure8: 不能提交已提交的日志
			LOG(rf.me, rf.currentTerm, DApply, "update the commit index %d->%d", rf.commitIndex, majorityMathced)
			rf.commitIndex = majorityMathced
			rf.applyCond.Signal()
		}

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1 // 未匹配日志标志位置，不包括本身
		prevTerm := rf.log.at(prevIdx).Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx + 1),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, append, args=%v", peer, args.String())
		go replicateToPeer(peer, args)
	}

	return true
}
