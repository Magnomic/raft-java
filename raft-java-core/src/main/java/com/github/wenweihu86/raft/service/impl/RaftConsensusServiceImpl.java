package com.github.wenweihu86.raft.service.impl;

import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.storage.Segment;
import com.github.wenweihu86.raft.util.ConfigurationUtils;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftConsensusServiceImpl implements RaftConsensusService {

    private static final Logger LOG = LoggerFactory.getLogger(RaftConsensusServiceImpl.class);
    private static final JsonFormat PRINTER = new JsonFormat();

    private RaftNode raftNode;

    public RaftConsensusServiceImpl(RaftNode node) {
        this.raftNode = node;
    }

    @Override
    public RaftProto.VoteResponse preVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build();
            }
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            boolean isLogOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (!isLogOk) {
                return responseBuilder.build();
            } else {
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
            }
            LOG.info("preVote request from server {} " +
                            "in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getGranted());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build();
            }
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            if (request.getTerm() > raftNode.getCurrentTerm()) {
                raftNode.stepDown(request.getTerm());
            }
            boolean logIsOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (raftNode.getVotedFor() == 0 && logIsOk) {
                raftNode.stepDown(request.getTerm());
                raftNode.setVotedFor(request.getServerId());
                raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(), raftNode.getVotedFor(), null, null);
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
            }
            LOG.info("RequestVote request from server {} " +
                            "in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getGranted());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request) {
        if (request.getFutureEntriesCount() != 0){
//            LOG.info("Append Future entries request received! {}. ", request.getFutureEntriesList());
            raftNode.getLock().lock();
            try{
                RaftProto.AppendEntriesResponse.Builder responseBuilder
                        = RaftProto.AppendEntriesResponse.newBuilder();
                responseBuilder.setTerm(raftNode.getCurrentTerm());
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
                // 发布Future entries的peer很有可能不知道当前的窗口在什么位置，告诉peer当前的窗口
                responseBuilder.setLastLogIndex(raftNode.getRaftFutureLog().getLastFutureLogIndex());
                // 告诉客户端当前的 generation 和 index
                responseBuilder.setFutureTerm(raftNode.getRaftFutureLog().getLastFutureGeneration());

                if (request.getFutureEntries(0).getFutureTerm() > raftNode.getRaftFutureLog().getLastFutureGeneration()){
                    // TODO: 让SegmentedLog关闭旧的窗口，并创建新的Segment
                    raftNode.getRaftFutureLog().truncateFuture(request.getFutureEntries(0).getFutureTerm(),
                            request.getFutureEntries(0).getIndex(), raftNode.getRaftLog().getLastLogIndex(),
                            raftNode.getLocalServer().getServerId());
                }

                long firstFutureEntryIndex = request.getFutureEntries(0).getIndex();
                Segment firstFutureEntrySegment = raftNode.getRaftFutureLog().getStartFutureLogIndexSegmentMap().
                        lowerEntry(firstFutureEntryIndex).getValue();
                if (firstFutureEntryIndex < firstFutureEntrySegment.getFutureGeneration()){
                    // 告诉来源，小于这个generation，且大于这个start index的我全都不认
                    responseBuilder.setLastLogIndex(firstFutureEntrySegment.getStartIndex());
                    responseBuilder.setFutureTerm(firstFutureEntrySegment.getFutureGeneration());
                    return responseBuilder.build();
                }

                // Term 不对的话，还是要返回的，不能接受
                if (request.getTerm() < raftNode.getCurrentTerm()) {
                    LOG.info("Term is wrong, remote term is {}, my term is {}",request.getTerm() ,raftNode.getCurrentTerm());
                    return responseBuilder.build();
                }
                // 判断当前的请求是否在当前窗口内
                if (request.getPrevLogIndex() < raftNode.getRaftLog().getLastLogIndex()){
                    LOG.info("Reject, window is passed, remote index is {}, my index is {}",request.getPrevLogIndex() ,raftNode.getRaftLog().getLastLogIndex());
                    return responseBuilder.build();
                }

                // 只要是在同一个Term，且在一个窗口内，写入log
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
                List<RaftProto.LogEntry> entries = new ArrayList<>();
                // index开始的地方
                long index = request.getPrevLogIndex();
                for (RaftProto.LogEntry entry : request.getFutureEntriesList()) {
                    if (firstFutureEntrySegment != null && index > firstFutureEntrySegment.getEndIndex()){
                        firstFutureEntrySegment = raftNode.getRaftFutureLog().getStartFutureLogIndexSegmentMap().
                                lowerEntry(index).getValue();
                    }
                    // 这里不会出现非窗口内的数据（理论上），所有也不会出现index小于普通Entries Index的情况
                    // 小于当前Segment中FirstLog的index都应当是非法的，直接跳过就可以
//                    if (index < raftNode.getRaftLog().getFirstLogIndex()) {
//                        continue;
//                    }
                    index = entry.getIndex();
//                    LOG.info("received index {} !", index);
                    // 如果这个Entry 已经添加过了就 pass掉
                    if (firstFutureEntrySegment != null && firstFutureEntrySegment.getFutureEntry(index) != null){
                        continue;
                    }
                    // 已经添加过的Log，就不用再加一遍了
                    // 而且不会truncate，因为Segment是定长的
//                    if (raftNode.getRaftLog().getLastLogIndex() >= index) {
//                        if (raftNode.getRaftLog().getEntryTerm(index) == entry.getTerm()) {
//
//                        }
//                        // truncate segment log from index
//                        long lastIndexKept = index - 1;
//                        raftNode.getRaftLog().truncateSuffix(lastIndexKept);
//                    }
                    entries.add(entry);
//                    index += raftNode.getConfiguration().getServersCount();
                }
                raftNode.getRaftFutureLog().appendFuture(raftNode.getConfiguration().getServersCount(),
                        raftNode.getLocalServer().getServerId(), raftNode.getRaftLog().getLastLogIndex(), entries);
//            raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(),
//                    null, raftNode.getRaftLog().getFirstLogIndex());
                responseBuilder.setLastLogIndex(index);

                // 不要commit
                advanceCommitFutureIndex(request);
//                LOG.info("AppendFutureEntries request from server {} " +
//                                "in term {} (my term is {}), entryCount={}, entries={}, resCode={}",
//                        request.getServerId(), request.getTerm(), raftNode.getCurrentTerm(),
//                        request.getFutureEntriesCount(), request.getFutureEntriesList(), responseBuilder.getResCode());
                return responseBuilder.build();

            } catch (Exception e){
                e.printStackTrace();
            } finally {
                raftNode.getLock().unlock();
            }
        }
        // 如果是Leader发来的请求，则在commit和apply的时候，检查是否包含了Future entries追加请求
        raftNode.getLock().lock();
        try {
            RaftProto.AppendEntriesResponse.Builder responseBuilder
                    = RaftProto.AppendEntriesResponse.newBuilder();
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
            responseBuilder.setLastFutureLogIndex(raftNode.getRaftFutureLog().getLastFutureLogIndex());
            // if leader's term is smaller than current term, tell leader current term and log index
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            // 首先确认当前节点退位，重置当选计时器
            raftNode.stepDown(request.getTerm());
            // 如果当前没有Leader，则认为请求者为Leader
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getServerId());
                LOG.info("new leaderId={}, conf={}",
                        raftNode.getLeaderId(),
                        PRINTER.printToString(raftNode.getConfiguration()));
            }
            // 如果当前节点收到了两个不同的Leader，在同一个Term中的请求
            // something went wrong, stop receiving logs and waiting for a new term
            if (raftNode.getLeaderId() != request.getServerId()) {
                LOG.warn("Another peer={} declares that it is the leader " +
                                "at term={} which was occupied by leader={}",
                        request.getServerId(), request.getTerm(), raftNode.getLeaderId());
                // 保证退位，等待新的Term
                raftNode.stepDown(request.getTerm() + 1);
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
                responseBuilder.setTerm(request.getTerm() + 1);
                return responseBuilder.build();
            }

            if (request.getPrevLogIndex() > raftNode.getRaftLog().getLastLogIndex()) {
                LOG.info("Rejecting AppendEntries RPC would leave gap, " +
                        "request prevLogIndex={}, my lastLogIndex={}",
                        request.getPrevLogIndex(), raftNode.getRaftLog().getLastLogIndex());
                return responseBuilder.build();
            }
            if (request.getPrevLogIndex() >= raftNode.getRaftLog().getFirstLogIndex()
                    && raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex())
                    != request.getPrevLogTerm()) {
                LOG.info("Rejecting AppendEntries RPC: terms don't agree, " +
                        "request prevLogTerm={} in prevLogIndex={}, my is {}",
                        request.getPrevLogTerm(), request.getPrevLogIndex(),
                        raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()));
                Validate.isTrue(request.getPrevLogIndex() > 0);
                responseBuilder.setLastLogIndex(request.getPrevLogIndex() - 1);
                return responseBuilder.build();
            }

            // 如果没有entries需要添加，那就是个心跳包
            if (request.getEntriesCount() == 0) {
                LOG.debug("heartbeat request from peer={} at term={}, my term={}",
                        request.getServerId(), request.getTerm(), raftNode.getCurrentTerm());
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
                responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
                advanceCommitIndex(request);
                return responseBuilder.build();
            }

            // 经过前面的判断，现在的entries应该在FirstIndex和LastIndex之间，且Term与Leader一致
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            long index = request.getPrevLogIndex();
//            LOG.info("{}", request.getEntriesList());
            for (RaftProto.LogEntry entry : request.getEntriesList()) {
//                // 跳过无意义的index
//                if (entry.getType().equals(RaftProto.EntryType.ENTRY_TYPE_EMPTY_DATA)){
//                    while (entry.getIndex() > index){
//                        index++;
//                    }
//                }
                index++;
                // 小于当前Segment中FirstLog的index都应当是非法的，直接跳过就可以
                if (index < raftNode.getRaftLog().getFirstLogIndex()) {
                    continue;
                }
                // 已经添加过的Log，就不用再加一遍了
                if (raftNode.getRaftLog().getLastLogIndex() >= index) {
                    if (raftNode.getRaftLog().getEntryTerm(index) == entry.getTerm()) {
                        continue;
                    }
                    // truncate segment log from index
                    long lastIndexKept = index - 1;
                    raftNode.getRaftLog().truncateSuffix(lastIndexKept);
                }
                if (entry.getType().equals(RaftProto.EntryType.ENTRY_TYPE_SIGNAL_DATA)){
//                    LOG.info("Signal index is {}", index);
//                    LOG.info("Signal info is {}", entry.getData());
                    // 如果以SIGNAL为结尾，说明当前index是一个future entry
                    entry = raftNode.getRaftFutureLog().getFutureEntry(index);
//                    LOG.info("now index is {}", index);
                    if (entry == null){
                        // 主节点有，我没有，我告诉主节点，我只Apply到当前index，主节点直接把这个Entry的完整内容重新发给我！
//                        LOG.info("I got SIGNAL DATA of {}, but I can not get its entry !!!", index);
//                        LOG.info("{}", raftNode.getRaftFutureLog().getStartFutureLogIndexSegmentMap().lowerEntry(index).getValue().getFutureEntries());
                        break;
                    }
                    Map<Long, Long> futureIndexMap = raftNode.getRaftFutureLog().getFutureIndexMap();
                    if (futureIndexMap.get(index) != null && futureIndexMap.get(index) !=  index){
                        Long iterator = futureIndexMap.get(index);
                        while (iterator < futureIndexMap.get(iterator)){
                            LOG.info("future trace: {}", iterator);
                            iterator = futureIndexMap.get(iterator);
                        }
                        LOG.info("future trace: {}", iterator);
                    }
                } else {
//                    LOG.info("check entry {}", index);
                    if (raftNode.getRaftFutureLog().getFutureEntry(index) != null &&
                            !entry.getType().equals(RaftProto.EntryType.ENTRY_TYPE_FUTURE_DATA)){
                        // 主节点没有这个Future Entry，但是我有
                        // ServerID是从1开始的，所以 serverID也要 mod Server Count
                        if (entry.getIndex() % raftNode.getConfiguration().getServersCount()
                                == raftNode.getLocalServer().getServerId() % raftNode.getConfiguration().getServersCount()){
//                            LOG.info("MEETS ENTRY LEADER MISSED!!! entry index is {}, data is {}", entry.getIndex(), entry);
                            // 如果是我自己产生的Future Entry，重新发布新的Future Entry
                            // 别人的就不要管了，让他自己去生成
                            // 写入Future Log
                            List<RaftProto.LogEntry> toDoEntries = new ArrayList<>();
                            // 重新分配 Future Index，然后不用管他就完了，下次发送Future Entries的时候，他就自己带着发出去了
                            RaftProto.LogEntry toDoEntry = RaftProto.LogEntry
                                    .newBuilder(raftNode.getRaftFutureLog().getFutureEntry(index))
                                    .setIndex(0)
                                    .build();
                            raftNode.getRaftFutureLog().getStartFutureLogIndexSegmentMap().
                                    lowerEntry(entry.getIndex()).getValue().removeFutureEntry(entry.getIndex());
                            toDoEntries.add(toDoEntry);
                            long newLastFutureLogIndex = raftNode.getRaftFutureLog().appendFuture(
                                    raftNode.getConfiguration().getServersCount(),
                                    raftNode.getLocalServer().getServerId(),
                                    raftNode.getRaftLog().getLastLogIndex(),
                                    toDoEntries);
                            raftNode.getRaftFutureLog().getFutureIndexMap().put(newLastFutureLogIndex , raftNode.getRaftFutureLog().getFutureIndexMap().get(index));
                            raftNode.getRaftFutureLog().getFutureIndexMap().put(index , newLastFutureLogIndex);
                        }
                    }
                    //
                }
                entries.add(entry);
            }
//            LOG.info("{}", entries);
            raftNode.getRaftLog().append(entries, raftNode.getRaftFutureLog());
//            raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(),
//                    null, raftNode.getRaftLog().getFirstLogIndex());
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());

            advanceCommitIndex(request);
            LOG.info("AppendEntries request from server {} " +
                            "in term {} (my term is {}), entryCount={} resCode={}",
                    request.getServerId(), request.getTerm(), raftNode.getCurrentTerm(),
                    request.getEntriesCount(), responseBuilder.getResCode());
            return responseBuilder.build();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            raftNode.getLock().unlock();
        }
        return null;
    }

    @Override
    public RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request) {
        RaftProto.InstallSnapshotResponse.Builder responseBuilder
                = RaftProto.InstallSnapshotResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);

        raftNode.getLock().lock();
        try {
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            raftNode.stepDown(request.getTerm());
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getServerId());
                LOG.info("new leaderId={}, conf={}",
                        raftNode.getLeaderId(),
                        PRINTER.printToString(raftNode.getConfiguration()));
            }
        } finally {
            raftNode.getLock().unlock();
        }

        if (raftNode.getSnapshot().getIsTakeSnapshot().get()) {
            LOG.warn("alreay in take snapshot, do not handle install snapshot request now");
            return responseBuilder.build();
        }

        raftNode.getSnapshot().getIsInstallSnapshot().set(true);
        RandomAccessFile randomAccessFile = null;
        raftNode.getSnapshot().getLock().lock();
        try {
            // write snapshot data to local
            String tmpSnapshotDir = raftNode.getSnapshot().getSnapshotDir() + ".tmp";
            File file = new File(tmpSnapshotDir);
            if (request.getIsFirst()) {
                if (file.exists()) {
                    file.delete();
                }
                file.mkdir();
                LOG.info("begin accept install snapshot request from serverId={}", request.getServerId());
                raftNode.getSnapshot().updateMetaData(tmpSnapshotDir,
                        request.getSnapshotMetaData().getLastIncludedIndex(),
                        request.getSnapshotMetaData().getLastIncludedTerm(),
                        request.getSnapshotMetaData().getConfiguration());
            }
            // write to file
            String currentDataDirName = tmpSnapshotDir + File.separator + "data";
            File currentDataDir = new File(currentDataDirName);
            if (!currentDataDir.exists()) {
                currentDataDir.mkdirs();
            }

            String currentDataFileName = currentDataDirName + File.separator + request.getFileName();
            File currentDataFile = new File(currentDataFileName);
            // 文件名可能是个相对路径，比如topic/0/message.txt
            if (!currentDataFile.getParentFile().exists()) {
                currentDataFile.getParentFile().mkdirs();
            }
            if (!currentDataFile.exists()) {
                currentDataFile.createNewFile();
            }
            randomAccessFile = RaftFileUtils.openFile(
                    tmpSnapshotDir + File.separator + "data",
                    request.getFileName(), "rw");
            randomAccessFile.seek(request.getOffset());
            randomAccessFile.write(request.getData().toByteArray());
            // move tmp dir to snapshot dir if this is the last package
            if (request.getIsLast()) {
                File snapshotDirFile = new File(raftNode.getSnapshot().getSnapshotDir());
                if (snapshotDirFile.exists()) {
                    FileUtils.deleteDirectory(snapshotDirFile);
                }
                FileUtils.moveDirectory(new File(tmpSnapshotDir), snapshotDirFile);
            }
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
            LOG.info("install snapshot request from server {} " +
                            "in term {} (my term is {}), resCode={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getResCode());
        } catch (IOException ex) {
            LOG.warn("when handle installSnapshot request, meet exception:", ex);
        } finally {
            RaftFileUtils.closeFile(randomAccessFile);
            raftNode.getSnapshot().getLock().unlock();
        }

        if (request.getIsLast() && responseBuilder.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
            // apply state machine
            // TODO: make this async
            String snapshotDataDir = raftNode.getSnapshot().getSnapshotDir() + File.separator + "data";
            raftNode.getStateMachine().readSnapshot(snapshotDataDir);
            long lastSnapshotIndex;
            // 重新加载snapshot
            raftNode.getSnapshot().getLock().lock();
            try {
                raftNode.getSnapshot().reload();
                lastSnapshotIndex = raftNode.getSnapshot().getMetaData().getLastIncludedIndex();
            } finally {
                raftNode.getSnapshot().getLock().unlock();
            }

            // discard old log entries
            raftNode.getLock().lock();
            try {
                raftNode.getRaftLog().truncatePrefix(lastSnapshotIndex + 1);
            } finally {
                raftNode.getLock().unlock();
            }
            LOG.info("end accept install snapshot request from serverId={}", request.getServerId());
        }

        if (request.getIsLast()) {
            raftNode.getSnapshot().getIsInstallSnapshot().set(false);
        }

        return responseBuilder.build();
    }

    // 事实上这里只是将收到的Future Log index 记录到 metadata中，并不是commit到state db中，
    // TODO: 先这样，用的时候再说
    private void advanceCommitFutureIndex(RaftProto.AppendEntriesRequest request) {
        long newCommitIndex = Math.max(raftNode.getRaftFutureLog().getStartFutureLogIndexSegmentMap().lastEntry().getValue().getEndIndex(),
                request.getPrevLogIndex()+ request.getEntriesCount());
//        raftNode.setCommitIndex(newCommitIndex);
        raftNode.getRaftFutureLog().updateMetaData(null,null, null, newCommitIndex);
//        if (raftNode.getLastAppliedIndex() < raftNode.getCommitIndex()) {
//            // apply state machine
//            for (long index = raftNode.getLastAppliedIndex() + 1;
//                 index <= raftNode.getCommitIndex(); index++) {
//                RaftProto.LogEntry entry = raftNode.getRaftLog().getEntry(index);
//                if (entry != null) {
//                    if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {
//                        raftNode.getStateMachine().apply(entry.getData().toByteArray());
//                    } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {
//                        raftNode.applyConfiguration(entry);
//                    }
//                }
//                raftNode.setLastAppliedIndex(index);
//            }
//        }
    }


    // in lock, for follower
    private void advanceCommitIndex(RaftProto.AppendEntriesRequest request) {
        long newCommitIndex = Math.min(request.getCommitIndex(),
                request.getPrevLogIndex() + request.getEntriesCount());
        raftNode.setCommitIndex(newCommitIndex);
        raftNode.getRaftLog().updateMetaData(null,null, null, newCommitIndex);
        LOG.info("raftNode.getLastAppliedIndex({}) , raftNode.getCommitIndex({})",raftNode.getLastAppliedIndex(), raftNode.getCommitIndex());
        if (raftNode.getLastAppliedIndex() < raftNode.getCommitIndex()) {
            // apply state machine
            for (long index = raftNode.getLastAppliedIndex() + 1;
                 index <= raftNode.getCommitIndex(); index++) {
                RaftProto.LogEntry entry = raftNode.getRaftLog().getEntry(index);
                if (entry != null && !entry.getType().equals(RaftProto.EntryType.ENTRY_TYPE_EMPTY_DATA)) {
                    if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA || entry.getType() == RaftProto.EntryType.ENTRY_TYPE_FUTURE_DATA) {
                        LOG.info("applying for {}", entry.getIndex());
                        if (entry.getIndex() % 1000 == 0){
                            if (raftNode.getLastTimePerK() == 0){
                                raftNode.setLastTimePerK(System.currentTimeMillis());
                            } else {
                                raftNode.setTpsPerK(System.currentTimeMillis() - raftNode.getLastTimePerK());
                                raftNode.setLastTimePerK(System.currentTimeMillis());
                            }
                        }
                        raftNode.getStateMachine().apply(entry.getData().toByteArray());
                    } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {
                        raftNode.applyConfiguration(entry);
                    }
                }
                raftNode.setLastAppliedIndex(index);
            }
        }
    }

}
