package raft

import (
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

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// peer's callback
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 如果prevlog未匹配上
	if args.PrevLogIndex >= len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, len:%d <=prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}

	// 说明从这里（或前面）开始日志丢失
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log, pre log not match, {%d}: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// 添加leader's log entries 到本地
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLeader, "follower accept logs: (%d %d)", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 处理leader commit
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}

	rf.resetElectionTimerLocked()
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

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, context lost, T%d:Leader->T%d:%d", peer, term, rf.currentTerm, rf.role)
			return
		}

		// 处理回复

		// 失败
		if !reply.Success {
			// 回退一个term
			idx, term := args.PrevLogIndex, args.PrevLogTerm
			for idx > 0 && rf.log[idx].Term == term {
				idx--
			}
			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, not matched at %d, try next=%d", peer, args.PrevLogIndex, rf.nextIndex[peer])
			return
		}

		// 成功

		// 更新匹配和next索引

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) // 不能使用自己的变量，因为可能会更新 ！！！
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// 更新提交索引
		majorityMathced := rf.getMajorityIndexLocked()
		if majorityMathced > rf.commitIndex {
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
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1 // 未匹配日志标志位置，不包括本身
		prevTerm := rf.log[prevIdx].Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:],
			LeaderCommit: rf.commitIndex,
		}

		go replicateToPeer(peer, args)
	}

	return true
}
