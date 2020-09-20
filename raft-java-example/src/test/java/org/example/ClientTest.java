package org.example;

import com.github.wenweihu86.raft.example.client.ClientMain;
import com.github.wenweihu86.raft.example.client.ConcurrentClientMain;
import com.github.wenweihu86.raft.storage.Segment;
import org.junit.Test;

import java.util.*;

public class ClientTest {
    private TreeMap<Long, Record> startFutureLogIndexSegmentMap = new TreeMap<>();
    class Record{
        private String id = "777";
        private String name;
        public Map<Long, Integer> futureEntries = new HashMap<Long, Integer>(100);
        public String getId(){
            return id;
        }
        public void setId(String id){
            this.id = id;
        }
    }
    @Test
    public void testClient() throws InterruptedException {
        String[] args = {"list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053"};
        ConcurrentClientMain.main(args);
//        String[] args = {"list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053", "f44fb884-89e7-4f61-b4fd-a18aed95b1f6"};
//        ClientMain.main(args);
//        long newLastLogIndex = 101;
//        int size = 3;
//        int serverId = 2;
//        newLastLogIndex = newLastLogIndex - size - (newLastLogIndex % size) + serverId;
//        System.out.println(newLastLogIndex);
//        Record record = new Reut.println(newLascord();
//        record.setId("123");
//        record.futureEntries.put(123L ,123);
//        startFutureLogIndexSegmentMap.put(123L, record);
//        record = startFutureLogIndexSegmentMap.get(123L);
//        record.setId("456");
//        record.futureEntries.put(123L ,456);
//        record = startFutureLogIndexSegmentMap.get(123L);
//        System.out.println(record.getId());
//        System.out.println(record.futureEntries.get(123L));
    }

}
