package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import com.github.wenweihu86.raft.proto.RaftProto;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Created by wenweihu86 on 2017/5/3.
 */
public class SegmentedLog {

    private static Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    private String logDir;
    private String logDataDir;
    private int maxSegmentFileSize;
    private RaftProto.LogMetaData metaData;
    private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();
    private TreeMap<Long, Segment> startFutureLogIndexSegmentMap = new TreeMap<>();
    private TreeMap<Long, Long> futureIndexMap = new TreeMap<>();
    // segment log占用的内存大小，用于判断是否需要做snapshot
    private volatile long totalSize;

    public SegmentedLog(String raftDataDir, String type, int maxSegmentFileSize) {
        this.logDir = raftDataDir + File.separator + "log" + type;
        this.logDataDir = logDir + File.separator + "data";
        this.maxSegmentFileSize = maxSegmentFileSize;
        File file = new File(logDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        LOG.info(file.getAbsolutePath());
        readSegments();
        for (Segment segment : startLogIndexSegmentMap.values()) {
            this.loadSegmentData(segment);
        }

        metaData = this.readMetaData();
        if (metaData == null) {
            if (startLogIndexSegmentMap.size() > 0) {
                LOG.error("No readable metadata file but found segments in {}", logDir);
//                throw new RuntimeException("No readable metadata file but found segments");
            }
            if ("future".equals(type)) {
                metaData = RaftProto.LogMetaData.newBuilder().setFirstLogIndex(Segment.fixedWindowSize).build();
            } else {
                metaData = RaftProto.LogMetaData.newBuilder().setFirstLogIndex(1).build();
            }
        }
    }

    public RaftProto.LogEntry getEntry(long index) {
        long firstLogIndex = getFirstLogIndex();
        long lastLogIndex = getLastLogIndex();
        if (index == 0 || index < firstLogIndex || index > lastLogIndex) {
            LOG.debug("index out of range, index={}, firstLogIndex={}, lastLogIndex={}",
                    index, firstLogIndex, lastLogIndex);
            return null;
        }
        if (startLogIndexSegmentMap.size() == 0) {
            return null;
        }
        // 找到小于等于index的最大Segment，即包含着index的segment
        Segment segment = startLogIndexSegmentMap.floorEntry(index).getValue();
        return segment.getEntry(index);
    }

    public RaftProto.LogEntry getFutureEntry(long index) {
        long firstLogIndex = getFirstLogIndex();
        long lastLogIndex = getLastFutureLogIndex();
//        LOG.info("index range: index={}, firstLogIndex={}, lastLogIndex={}",
//                index, firstLogIndex, lastLogIndex);
        if (index == 0 || index < firstLogIndex || index > lastLogIndex) {
            LOG.info("index out of range, index={}, firstLogIndex={}, lastLogIndex={}",
                    index, firstLogIndex, lastLogIndex);
            return null;
        }
        if (startFutureLogIndexSegmentMap.size() == 0 || startFutureLogIndexSegmentMap.floorEntry(index) == null) {
            LOG.info("Future segment map is null!!!");
            return null;
        }
        // 找到小于等于index的最大Segment，即包含着index的segment
        Segment segment = startFutureLogIndexSegmentMap.floorEntry(index).getValue();
//        LOG.info("This is all of my entries! {}", segment.getFutureEntries());
        return segment.getFutureEntry(index);
    }

    public long getEntryTerm(long index) {
        RaftProto.LogEntry entry = getEntry(index);
        if (entry == null) {
            return 0;
        }
        return entry.getTerm();
    }

    public long getFirstLogIndex() {
        return metaData.getFirstLogIndex();
    }

    public long getLastLogIndex() {
        // 有两种情况segment为空
        // 1、第一次初始化，firstLogIndex = 1，lastLogIndex = 0
        // 2、snapshot刚完成，日志正好被清理掉，firstLogIndex = snapshotIndex + 1， lastLogIndex = snapshotIndex
        if (startLogIndexSegmentMap.size() == 0) {
            return getFirstLogIndex() - 1;
        }
        Segment lastSegment = startLogIndexSegmentMap.lastEntry().getValue();
        return lastSegment.getEndIndex();
    }

    public long getCurrentFutureWindow() {
        // 有两种情况segment为空
        // 1、第一次初始化，firstLogIndex = 1，lastLogIndex = 0
        // 2、snapshot刚完成，日志正好被清理掉，firstLogIndex = snapshotIndex + 1， lastLogIndex = snapshotIndex
        if (startFutureLogIndexSegmentMap.size() == 0) {
            return getFirstLogIndex() - 1;
        }
        return startFutureLogIndexSegmentMap.lastEntry().getKey();
//        return lastSegment.getEndIndex();
    }

    public long getLastFutureGeneration() {
        // 有两种情况segment为空
        // 1、第一次初始化，firstLogIndex = 1，lastLogIndex = 0
        // 2、snapshot刚完成，日志正好被清理掉，firstLogIndex = snapshotIndex + 1， lastLogIndex = snapshotIndex
        if (startFutureLogIndexSegmentMap.size() == 0) {
            return 0;
        }
        return startFutureLogIndexSegmentMap.lastEntry().getValue().getFutureGeneration();
//        return lastSegment.getEndIndex();
    }

    public long getLastFutureLogIndex() {
        // 有两种情况segment为空
        // 1、第一次初始化，firstLogIndex = 1，lastLogIndex = 0
        // 2、snapshot刚完成，日志正好被清理掉，firstLogIndex = snapshotIndex + 1， lastLogIndex = snapshotIndex
        if (startFutureLogIndexSegmentMap.size() == 0) {
            return getFirstLogIndex() - 1;
        }
        Segment lastSegment = startFutureLogIndexSegmentMap.lastEntry().getValue();
        return lastSegment.getEndIndex();
    }

    public long appendFuture(int size, int serverId, long nowIndex, List<RaftProto.LogEntry> entries){
//        if (startLogIndexSegmentMap.size() ==0){
//            return getFirstLogIndex() - 1;
//        }
//        LOG.info("received future entries {}, now index {}", entries, nowIndex);
        long newLastLogIndex =  0;
//        LOG.info("appendFuture invoked! Entries size is {}, server size is {}, server id is {}", entries.size(), size, serverId);
        // TODO: 如果接受到了Fixed Window Size的整数倍Index，则创建新的Segment，关闭旧的Segment
        if (startFutureLogIndexSegmentMap.size() == 0){
            newLastLogIndex = Segment.fixedWindowSize - 1;
            // 向上取整周期并+server id
//            LOG.info("newLastLogIndex = {} + {} - ({} % {}) + {}", newLastLogIndex, size, newLastLogIndex, size, serverId);
            newLastLogIndex = newLastLogIndex - (newLastLogIndex % size) + (serverId % size);
        } else {
            // 由于自己发布的index自己是知道的，且周期性增加index，即使不知道其他节点发布的最新index，也不会产生冲突
//            for (Map.Entry<Long, RaftProto.LogEntry> entry : futureLogData.entrySet()){
//                LOG.info("key : {}, value : {}", entry.getKey(), entry.getValue());
//            }
            // 获取本Segment当中，对应本serverId的待插入index
            // TODO: 在插入Future index时，将对应的serverId的LastIndex更新
            newLastLogIndex = getLastFutureLogIndex();
//            LOG.info("newLastLogIndex = {} + {} - ({} % {}) + {}", newLastLogIndex, size, newLastLogIndex, size, serverId);
            newLastLogIndex = newLastLogIndex - (newLastLogIndex % size) + (serverId % size);
            LOG.info("old newLastLogIndex is {}", newLastLogIndex);
        }
//        LOG.info("Time went here : {}", System.currentTimeMillis() - startTime);
        for (RaftProto.LogEntry entry : entries) {
//            LOG.info("going to apply {}", entry);
            // 如果没有index， 那么说明是自己添加的， 分配 index
            if (entry.getIndex() == 0){
                // 保证Server间提交的index不冲突
                newLastLogIndex += size;
            } else {
                newLastLogIndex = entry.getIndex();
            }

            int entrySize = entry.getSerializedSize();
            long segmentGeneration = size;
            int newSegmentStartIndex = 0;
            int segmentSize = startFutureLogIndexSegmentMap.size();
            boolean isNeedNewSegmentFile = false;
            long lastEndIndex = Segment.fixedWindowSize - 1;
            try {
                if (segmentSize == 0) {
                    isNeedNewSegmentFile = true;
                    while (lastEndIndex < nowIndex + Segment.fixedWindowSize){
                        lastEndIndex += Segment.fixedWindowSize;
                    }
//                    lastEndIndex-=1;
                    if (entry.getIndex() == 0) {
                        // 如果是别的节点传过来的 entry，不要改变他的 index
                        newLastLogIndex = lastEndIndex + size - (lastEndIndex % size) + serverId;
                    }
                } else {
                    Segment segment;
                    // entry index 分配方法
                    if (entry.getIndex() == 0) {
                        // 新建的就往后扔
                        segment = startFutureLogIndexSegmentMap.lastEntry().getValue();

                        segmentGeneration = segment.getFutureGeneration();
                        // 如果这个Entry没有index，则为它分配新窗口中的index
//                    LOG.info("I am trying to put entry to segment {}-{}, can write ? {}, in same window ? {}", segment.getStartIndex(), segment.getEndIndex(),
//                            segment.isCanWrite(), nowIndex / Segment.fixedWindowSize == newLastLogIndex / Segment.fixedWindowSize);
                        if (newLastLogIndex - segment.getStartIndex() >= Segment.fixedWindowSize
                                // size > segmentGeneration是Leader主动告知的
                                // entry.getFutureTerm > segmentGeneration，是其他Follower告知的
                                // 但不管如何，都应当关掉之前的窗口，把之前窗口中的错误数据创建新的窗口
                                || !segment.isCanWrite() || size > segmentGeneration
                                || nowIndex / Segment.fixedWindowSize == newLastLogIndex / Segment.fixedWindowSize) {
                            // 已经超出窗口，但是不要关掉旧的Segment，因为还要等待其它节点的Entries
                            // last end index 只能作为上一个窗口的结束位置，可能会跳很多窗口，如果之前的newLastLogIndex不合理的话，也要跟着跳
                            lastEndIndex = segment.getStartIndex() - 1 + Segment.fixedWindowSize;

                            segmentGeneration = size;
                            // 超出窗口但是也不一定就是合法的，因为中间可能隔了很多个窗口
                            // 至少应该在当前index的下一个窗口里
                            while (lastEndIndex + Segment.fixedWindowSize < newLastLogIndex
                                    || lastEndIndex < nowIndex + Segment.fixedWindowSize - nowIndex % Segment.fixedWindowSize){
                                LOG.info("lastEndIndex {} < nowIndex {} + Segment.fixedWindowSize {} - nowIndex {}" +
                                                " % Segment.fixedWindowSize {} = {}", lastEndIndex, nowIndex, Segment.fixedWindowSize, nowIndex, Segment.fixedWindowSize,
                                        lastEndIndex < nowIndex + Segment.fixedWindowSize - nowIndex % Segment.fixedWindowSize);
                                lastEndIndex += Segment.fixedWindowSize;
                            }

                            isNeedNewSegmentFile = true;

                            LOG.info("newLastLogIndex is {}, segment.getStartIndex is {}, lastEndIndex is {}", newLastLogIndex, segment.getStartIndex(), lastEndIndex);
                            newLastLogIndex = lastEndIndex + size - (lastEndIndex % size) + (serverId % size);

                            // Future Entries不是在这个时候关闭它的Segment，而是在index已经达到这个Segment的最低窗口时，才关闭这个Segment

                        } else {
                            // 啥都没变的话，直接取就行
                            newLastLogIndex = segment.getEndIndex();
                            newLastLogIndex = newLastLogIndex + size - (newLastLogIndex % size) + (serverId % size);
                        }
                    } else {
                        // 不是新建就找到对应的 segment
                        segment = startFutureLogIndexSegmentMap.lowerEntry(entry.getIndex()).getValue();
                        // 同一个窗口的话，不写
                        if (nowIndex / Segment.fixedWindowSize == newLastLogIndex / Segment.fixedWindowSize){
                            continue;
                        }
                        long lastStartIndex = segment.getStartIndex();
                        // 如果这个segment的结束index不正确
                        while (entry.getIndex() - lastStartIndex >= Segment.fixedWindowSize){
                            isNeedNewSegmentFile = true;
                            lastStartIndex += Segment.fixedWindowSize;
                        }
                        lastEndIndex = lastStartIndex - 1;
                    }

                }
                Segment newSegment;
                // 新建segment文件
                if (isNeedNewSegmentFile) {
                    LOG.info("Create New Future segment");
//                    LOG.info("segmentSize is {}", segmentSize);
                    // open new segment file
                    String newSegmentFileName = String.format("open-%d-%d", size, lastEndIndex + 1);
                    String newFullFileName = logDataDir + File.separator + newSegmentFileName;
                    File newSegmentFile = new File(newFullFileName);
                    if (!newSegmentFile.exists()) {
                        newSegmentFile.createNewFile();
                    }
                    Segment segment = new Segment();
                    segment.setCanWrite(true);
                    segment.setStartIndex(lastEndIndex + 1);
                    segment.setEndIndex(0);
                    segment.setFileName(newSegmentFileName);
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newSegmentFileName, "rw"));
                    segment.setFutureGeneration(segmentGeneration);
                    newSegment = segment;
                } else {
                    newSegment = startFutureLogIndexSegmentMap.lastEntry().getValue();
                }
                // 写index到entry中
                if (entry.getIndex() == 0) {
                    entry = RaftProto.LogEntry.newBuilder(entry)
                            .setIndex(newLastLogIndex).setFutureTerm(size).build();
                    futureIndexMap.put(newLastLogIndex, newLastLogIndex);
                }
                LOG.info("I will put index {} to segment {}-{}", entry.getIndex(), newSegment.getStartIndex(), newSegment.getEndIndex());
                newSegment.setEndIndex(Math.max(newSegment.getEndIndex(), entry.getIndex()));
                newSegment.putEntry(entry.getIndex(), new Segment.Record(
                        newSegment.getRandomAccessFile().getFilePointer(), entry));
                RaftFileUtils.writeProtoToFile(newSegment.getRandomAccessFile(), entry);
                newSegment.setFileSize(newSegment.getRandomAccessFile().length());
                if (!startFutureLogIndexSegmentMap.containsKey(newSegment.getStartIndex())) {
                    startFutureLogIndexSegmentMap.put(newSegment.getStartIndex(), newSegment);
                }
                totalSize += entrySize;
            }  catch (IOException ex) {
                ex.printStackTrace();
                throw new RuntimeException("append raft log exception, msg=" + ex.getMessage());
            }
//            LOG.info("Time went here : {}", System.currentTimeMillis() - startTime);
        }
        return newLastLogIndex;
    }

    // 这是个公用的方法，Leader也用，Follower也用，Leader的Entries参数只有一个Entry，且没有index，但是Follower的可能有多个，且都有index
    // Leader : N
    // Follower: NFFFFFF 或 FFFFFF 或 F 或 NNNNNNN
    public long append(List<RaftProto.LogEntry> entries, SegmentedLog futureLog) {
        long newLastLogIndex = this.getLastLogIndex();
        // 如果当前位置是有future的
        // 如果当前entry没有index，而且当前index有Future index占用
        List<RaftProto.LogEntry> extraFutureEntries = new ArrayList<>();
        while (entries.size()!=0 && entries.get(0).getIndex() == 0 && futureLog.startFutureLogIndexSegmentMap.size() != 0
                && futureLog.startFutureLogIndexSegmentMap.lowerEntry(newLastLogIndex + 1) != null) {
            // 找到包含这个future 的 segment
            Segment futureSegment = futureLog.startFutureLogIndexSegmentMap.lowerEntry(newLastLogIndex + 1).getValue();
//            LOG.info("the list of future entries are : {}", futureSegment.getFutureEntries());
            // 取出来
            RaftProto.LogEntry ifFutureEntry = futureSegment.getFutureEntry(newLastLogIndex + 1);
            if (ifFutureEntry != null) {
                LOG.info("newLastLogIndex {} is a Future Entry, Swap the index and put it to Entries", newLastLogIndex + 1);
                // 把当前的future entry放到最前面
                extraFutureEntries.add(ifFutureEntry);
            } else {
                break;
            }
            newLastLogIndex ++;
        }
        entries.addAll(0, extraFutureEntries);
        newLastLogIndex = this.getLastLogIndex();
        for (RaftProto.LogEntry entry : entries) {
            newLastLogIndex++;
//            LOG.info("My future map is {}", futureLog.startFutureLogIndexSegmentMap.keySet());
            LOG.info("entry.getIndex() =  {}", entry.getIndex());
//            LOG.info("lowerEntry of future map is {}", futureLog.startFutureLogIndexSegmentMap.lowerEntry(newLastLogIndex));

            int entrySize = entry.getSerializedSize();
            int segmentSize = startLogIndexSegmentMap.size();
            boolean isNeedNewSegmentFile = false;
            try {
                // 如果超出了窗口，而且这个窗口还没关
                Map.Entry<Long, Segment> futureSegmentEntry = futureLog.startFutureLogIndexSegmentMap.lowerEntry(newLastLogIndex);
                if (futureSegmentEntry != null) {
//                    LOG.info("{},{},{},{}", futureSegmentEntry, newLastLogIndex, futureSegmentEntry.getKey(),
//                            futureSegmentEntry.getValue().isCanWrite());
                }
                if (futureSegmentEntry != null && futureSegmentEntry.getValue().isCanWrite()) {
                    Segment segment = futureSegmentEntry.getValue();
//                    LOG.info("segment {}-{} is closing", segment.getStartIndex(), segment.getEndIndex());
                    // 已经超出窗口，关掉旧的Segment，不再接受任何Entries
                    segment.getRandomAccessFile().close();
                    segment.setCanWrite(false);
                    String newFileName = String.format("Future-%d-%020d-%020d", segment.getFutureGeneration(),
                            segment.getStartIndex(), segment.getEndIndex());
//                    LOG.info("content are : {}", segment.getFutureEntries());
                    String newFullFileName = futureLog.logDataDir + File.separator + newFileName;
                    File newFile = new File(newFullFileName);
                    String oldFullFileName = futureLog.logDataDir + File.separator + segment.getFileName();
                    File oldFile = new File(oldFullFileName);
                    FileUtils.moveFile(oldFile, newFile);
                    segment.setFileName(newFileName);
                    segment.setRandomAccessFile(RaftFileUtils.openFile(futureLog.logDataDir, newFileName, "r"));
                }
                if (segmentSize == 0) {
                    isNeedNewSegmentFile = true;
                } else {
                    Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
                    if (!segment.isCanWrite()) {
                        isNeedNewSegmentFile = true;
                    } else if (segment.getFileSize() + entrySize >= maxSegmentFileSize) {
                        isNeedNewSegmentFile = true;
                        // 最后一个segment的文件close并改名
                        segment.getRandomAccessFile().close();
                        segment.setCanWrite(false);
                        String newFileName = String.format("%020d-%020d",
                                segment.getStartIndex(), segment.getEndIndex());
                        String newFullFileName = logDataDir + File.separator + newFileName;
                        File newFile = new File(newFullFileName);
                        String oldFullFileName = logDataDir + File.separator + segment.getFileName();
                        File oldFile = new File(oldFullFileName);
                        FileUtils.moveFile(oldFile, newFile);
                        segment.setFileName(newFileName);
                        segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newFileName, "r"));
                        LOG.info("end old segment, its size is {}, entry size is {}, max size is {}",
                                segment.getFileSize(), entrySize, maxSegmentFileSize);
                    }
                }
                Segment newSegment;
                // 新建segment文件
                if (isNeedNewSegmentFile) {
                    // open new segment file
                    String newSegmentFileName = String.format("open-%d", newLastLogIndex);
                    String newFullFileName = logDataDir + File.separator + newSegmentFileName;
                    File newSegmentFile = new File(newFullFileName);
                    if (!newSegmentFile.exists()) {
                        newSegmentFile.createNewFile();
                    }
                    Segment segment = new Segment();
                    segment.setCanWrite(true);
                    segment.setStartIndex(newLastLogIndex);
                    segment.setEndIndex(0);
                    segment.setFileName(newSegmentFileName);
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newSegmentFileName, "rw"));
                    newSegment = segment;
                    LOG.info("Create new segment, its path is {}", newSegmentFile.getAbsolutePath());
                } else {
                    newSegment = startLogIndexSegmentMap.lastEntry().getValue();
                }
                // 写proto到segment中
                if (entry.getIndex() == 0) {
                    entry = RaftProto.LogEntry.newBuilder(entry)
                            .setIndex(newLastLogIndex).build();
                }
                newSegment.setEndIndex(entry.getIndex());
                newSegment.getEntries().add(new Segment.Record(
                        newSegment.getRandomAccessFile().getFilePointer(), entry));
//                LOG.info("Entry {} applied!", entry.getIndex());
                RaftFileUtils.writeProtoToFile(newSegment.getRandomAccessFile(), entry);
                newSegment.setFileSize(newSegment.getRandomAccessFile().length());
                if (!startLogIndexSegmentMap.containsKey(newSegment.getStartIndex())) {
                    startLogIndexSegmentMap.put(newSegment.getStartIndex(), newSegment);
                }
                totalSize += entrySize;
            }  catch (IOException ex) {
                ex.printStackTrace();
                throw new RuntimeException("append raft log exception, msg=" + ex.getMessage());
            }
        }
        return newLastLogIndex;
    }

    // in lock
    public void truncateFuture(long generation, long firstIndex, long loggedNormalIndex, int serverId){
        LOG.info("generation {}, firstIndex {}, loggedNormalIndex {}, serverId {}", generation, firstIndex, loggedNormalIndex, serverId);
        // 找到当前要废掉的窗口
        Segment segment;
        List<RaftProto.LogEntry> pendingEntries = new ArrayList<>();
        List<Long> indexList = new ArrayList<>();
        if (startFutureLogIndexSegmentMap.lowerEntry(firstIndex) != null) {
            Map.Entry<Long, Segment> segmentEntry = startFutureLogIndexSegmentMap.lowerEntry(firstIndex);
            while (segmentEntry != null) {
                segment = segmentEntry.getValue();
                if (segment.getFutureGeneration() < generation) {
                    // 取出里面的所有数据，对于大于已经apply的index的日志条目，作废并重新加入新的segment
                    for (Segment.Record logEntry : segment.getFutureEntries()) {
                        if (logEntry.entry != null && logEntry.entry.getIndex() > loggedNormalIndex &&
                                logEntry.entry.getIndex() % segment.getFutureGeneration() == serverId) {
                            pendingEntries.add(RaftProto.LogEntry
                                    .newBuilder(logEntry.entry)
                                    .setIndex(0)
                                    .build());
                            indexList.add(logEntry.entry.getIndex());
                        }
                    }
                    // 作废了的直接删掉就完事了，留着也没啥用
                    startFutureLogIndexSegmentMap.remove(startFutureLogIndexSegmentMap.lowerEntry(firstIndex).getKey());
                    // 文件也删掉，省的恢复了
                    File segmentFile = new File(segment.getFileName());
                    FileUtils.deleteQuietly(segmentFile);
                }
                // 搞下一个
                segmentEntry = startFutureLogIndexSegmentMap.lowerEntry(firstIndex + Segment.fixedWindowSize);
            }
        }
        LOG.info("Create New Future segment");
        // 创建一个以更高代数的窗口，并创建文件
        long startIndex = firstIndex - firstIndex % Segment.fixedWindowSize;
        String newSegmentFileName = String.format("open-%d-%d", generation, startIndex);
        String newFullFileName = logDataDir + File.separator + newSegmentFileName;
        File newSegmentFile = new File(newFullFileName);
        if (!newSegmentFile.exists()) {
            try {
                newSegmentFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        segment = new Segment();
        segment.setCanWrite(true);
        segment.setStartIndex(startIndex);
        segment.setEndIndex(0);
        segment.setFileName(newSegmentFileName);
        segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newSegmentFileName, "rw"));
        segment.setFutureGeneration(generation);
        startFutureLogIndexSegmentMap.put(segment.getStartIndex(), segment);
        long lastFutureLogIndex = appendFuture((int) generation, serverId, loggedNormalIndex, pendingEntries);
        for (int i = 0; i < pendingEntries.size(); i++){
            futureIndexMap.put(lastFutureLogIndex - i * generation, futureIndexMap.get(indexList.get(pendingEntries.size() - i - 1)));
            futureIndexMap.put(indexList.get(pendingEntries.size() - i - 1), lastFutureLogIndex - i * generation);
        }
    }

    public void truncatePrefix(long newFirstIndex) {
        if (newFirstIndex <= getFirstLogIndex()) {
            return;
        }
        long oldFirstIndex = getFirstLogIndex();
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.firstEntry().getValue();
            if (segment.isCanWrite()) {
                break;
            }
            if (newFirstIndex > segment.getEndIndex()) {
                File oldFile = new File(logDataDir + File.separator + segment.getFileName());
                try {
                    RaftFileUtils.closeFile(segment.getRandomAccessFile());
                    FileUtils.forceDelete(oldFile);
                    totalSize -= segment.getFileSize();
                    startLogIndexSegmentMap.remove(segment.getStartIndex());
                } catch (Exception ex2) {
                    LOG.warn("delete file exception:", ex2);
                }
            } else {
                break;
            }
        }
        long newActualFirstIndex;
        if (startLogIndexSegmentMap.size() == 0) {
            newActualFirstIndex = newFirstIndex;
        } else {
            newActualFirstIndex = startLogIndexSegmentMap.firstKey();
        }
        updateMetaData(null, null, newActualFirstIndex, null);
        LOG.info("Truncating log from old first index {} to new first index {}",
                oldFirstIndex, newActualFirstIndex);
    }

    public void truncateSuffix(long newEndIndex) {
        if (newEndIndex >= getLastLogIndex()) {
            return;
        }
        LOG.info("Truncating log from old end index {} to new end index {}",
                getLastLogIndex(), newEndIndex);
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
            try {
                if (newEndIndex == segment.getEndIndex()) {
                    break;
                } else if (newEndIndex < segment.getStartIndex()) {
                    totalSize -= segment.getFileSize();
                    // delete file
                    segment.getRandomAccessFile().close();
                    String fullFileName = logDataDir + File.separator + segment.getFileName();
                    FileUtils.forceDelete(new File(fullFileName));
                    startLogIndexSegmentMap.remove(segment.getStartIndex());
                } else if (newEndIndex < segment.getEndIndex()) {
                    int i = (int) (newEndIndex + 1 - segment.getStartIndex());
                    segment.setEndIndex(newEndIndex);
                    long newFileSize = segment.getEntries().get(i).offset;
                    totalSize -= (segment.getFileSize() - newFileSize);
                    segment.setFileSize(newFileSize);
                    segment.getEntries().removeAll(
                            segment.getEntries().subList(i, segment.getEntries().size()));
                    FileChannel fileChannel = segment.getRandomAccessFile().getChannel();
                    fileChannel.truncate(segment.getFileSize());
                    fileChannel.close();
                    segment.getRandomAccessFile().close();
                    String oldFullFileName = logDataDir + File.separator + segment.getFileName();
                    String newFileName = String.format("%020d-%020d",
                            segment.getStartIndex(), segment.getEndIndex());
                    segment.setFileName(newFileName);
                    String newFullFileName = logDataDir + File.separator + segment.getFileName();
                    new File(oldFullFileName).renameTo(new File(newFullFileName));
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, segment.getFileName(), "rw"));
                }
            } catch (IOException ex) {
                LOG.warn("io exception, msg={}", ex.getMessage());
            }
        }
    }

    public void loadSegmentData(Segment segment) {
        try {
            RandomAccessFile randomAccessFile = segment.getRandomAccessFile();
            long totalLength = segment.getFileSize();
            long offset = 0;
            while (offset < totalLength) {
                RaftProto.LogEntry entry = RaftFileUtils.readProtoFromFile(
                        randomAccessFile, RaftProto.LogEntry.class);
                if (entry == null) {
                    throw new RuntimeException("read segment log failed");
                }
                Segment.Record record = new Segment.Record(offset, entry);
                segment.getEntries().add(record);
                offset = randomAccessFile.getFilePointer();
            }
            totalSize += totalLength;
        } catch (Exception ex) {
            LOG.error("read segment meet exception, msg={}", ex.getMessage());
            throw new RuntimeException("file not found");
        }

        int entrySize = segment.getEntries().size();
        if (entrySize > 0) {
            segment.setStartIndex(segment.getEntries().get(0).entry.getIndex());
            segment.setEndIndex(segment.getEntries().get(entrySize - 1).entry.getIndex());
        }
    }

    public void readSegments() {
        try {
            List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(logDataDir, logDataDir);
            for (String fileName : fileNames) {
                String[] splitArray = fileName.split("-");
                if (splitArray.length != 2) {
                    LOG.warn("segment filename[{}] is not valid", fileName);
                    continue;
                }
                Segment segment = new Segment();
                segment.setFileName(fileName);
                if (splitArray[0].equals("open")) {
                    segment.setCanWrite(true);
                    segment.setStartIndex(Long.valueOf(splitArray[1]));
                    segment.setEndIndex(0);
                } else {
                    try {
                        segment.setCanWrite(false);
                        segment.setStartIndex(Long.parseLong(splitArray[0]));
                        segment.setEndIndex(Long.parseLong(splitArray[1]));
                    } catch (NumberFormatException ex) {
                        LOG.warn("segment filename[{}] is not valid", fileName);
                        continue;
                    }
                }
                segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, fileName, "rw"));
                segment.setFileSize(segment.getRandomAccessFile().length());
                startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
            }
        } catch(IOException ioException){
            LOG.warn("readSegments exception:", ioException);
            throw new RuntimeException("open segment file error");
        }
    }

    public RaftProto.LogMetaData readMetaData() {
        String fileName = logDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            RaftProto.LogMetaData metadata = RaftFileUtils.readProtoFromFile(
                    randomAccessFile, RaftProto.LogMetaData.class);
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    /**
     * 更新raft log meta data，
     * 包括commitIndex， fix bug: https://github.com/wenweihu86/raft-java/issues/19
     * @param currentTerm
     * @param votedFor
     * @param firstLogIndex
     * @param commitIndex
     */
    public void updateMetaData(Long currentTerm, Integer votedFor, Long firstLogIndex, Long commitIndex) {
        RaftProto.LogMetaData.Builder builder = RaftProto.LogMetaData.newBuilder(this.metaData);
        if (currentTerm != null) {
            builder.setCurrentTerm(currentTerm);
        }
        if (votedFor != null) {
            builder.setVotedFor(votedFor);
        }
        if (firstLogIndex != null) {
            builder.setFirstLogIndex(firstLogIndex);
        }
        if (commitIndex != null) {
            builder.setCommitIndex(commitIndex);
        }
        this.metaData = builder.build();

        String fileName = logDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            RaftFileUtils.writeProtoToFile(randomAccessFile, metaData);
//            LOG.info("new segment meta info, currentTerm={}, votedFor={}, firstLogIndex={}",
//                    metaData.getCurrentTerm(), metaData.getVotedFor(), metaData.getFirstLogIndex());
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
        }
    }

    public RaftProto.LogMetaData getMetaData() {
        return metaData;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public TreeMap<Long, Segment> getStartFutureLogIndexSegmentMap() {
        return startFutureLogIndexSegmentMap;
    }

    public void setStartFutureLogIndexSegmentMap(TreeMap<Long, Segment> startFutureLogIndexSegmentMap) {
        this.startFutureLogIndexSegmentMap = startFutureLogIndexSegmentMap;
    }

    public TreeMap<Long, Long> getFutureIndexMap() {
        return futureIndexMap;
    }

    public void setFutureIndexMap(TreeMap<Long, Long> futureIndexMap) {
        this.futureIndexMap = futureIndexMap;
    }
}
