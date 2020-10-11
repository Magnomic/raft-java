package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.proto.RaftProto;

import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by wenweihu86 on 2017/5/3.
 */
public class Segment {


    public static class Record {
        public long offset;
        public RaftProto.LogEntry entry;
        public Record(long offset, RaftProto.LogEntry entry) {
            this.offset = offset;
            this.entry = entry;
        }

        @Override
        public String toString() {
            return String.format("offset : %d, entry: %s", offset, entry.toString());
        }
    }

    public static int fixedWindowSize = 400;
    private long futureEndIndex;
    private boolean canWrite;
    private long startIndex;
    private long endIndex;
    private long fileSize;
    private long futureGeneration;
    private String fileName;
    private RandomAccessFile randomAccessFile;
    private List<Record> entries = new ArrayList<>();
    private List<Record> futureEntries = new ArrayList<>(Collections.nCopies(fixedWindowSize, new Record(-1, null)));

    public RaftProto.LogEntry getEntry(long index) {
        if (startIndex == 0 || endIndex == 0) {
            return null;
        }
        if (index < startIndex || index > endIndex) {
            return null;
        }
        int indexInList = (int) (index - startIndex);
        return entries.get(indexInList).entry;
    }

    public RaftProto.LogEntry getFutureEntry(long index) {
        int innerIndex = (int) (index % fixedWindowSize);
        if (index - startIndex > fixedWindowSize) return null;
        if (futureEntries.get(innerIndex).entry == null) return null;
        if (futureEntries.get(innerIndex).entry.getIndex() != index) {
//            System.out.println(String.format("!!!!!!!!!!!!!!!! entry not same, %d %d",futureEntries.get(innerIndex).entry.getIndex(),index));
            return null;
        }
        return futureEntries.get(innerIndex).entry;
    }

    public void putEntry(long index, Record record){
        int innerIndex = (int) (index % fixedWindowSize);
        futureEntries.set(innerIndex, record);
    }

    public void removeFutureEntry(long index) {
        int innerIndex = (int) (index % fixedWindowSize);
        futureEntries.set(innerIndex, new Record(-1, null));
    }

    public boolean isCanWrite() {
        return canWrite;
    }

    public void setCanWrite(boolean canWrite) {
        this.canWrite = canWrite;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(long startIndex) {
        this.startIndex = startIndex;
    }

    public long getEndIndex() {
        return endIndex;
    }

    public long getFutureSegmentEndIndex(int serverId){
        return futureEndIndex;
    }

    public void setEndIndex(long endIndex) {
        this.endIndex = endIndex;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public RandomAccessFile getRandomAccessFile() {
        return randomAccessFile;
    }

    public void setRandomAccessFile(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    public List<Record> getEntries() {
        return entries;
    }

    public void setEntries(List<Record> entries) {
        this.entries = entries;
    }

    public long getFutureGeneration() {
        return futureGeneration;
    }

    public void setFutureGeneration(long futureGeneration) {
        this.futureGeneration = futureGeneration;
    }

    public List<Record> getFutureEntries() {
        return futureEntries;
    }

    public void setFutureEntries(List<Record> futureEntries) {
        this.futureEntries = futureEntries;
    }
}
