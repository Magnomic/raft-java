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
//        List<Record> records = new ArrayList<>(Collections.nCopies(100, new Record()));
//        System.out.println(records.get(10));
//        String[] args = {"list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053,127.0.0.1:8054,127.0.0.1:8055"};
//        String[] args = {"list://127.0.0.1:8061,127.0.0.1:8062,127.0.0.1:8063,127.0.0.1:8064,127.0.0.1:8065"};
//        String[] args = {"list://127.0.0.1:8061,127.0.0.1:8062,127.0.0.1:8063"};
//        System.out.println(Runtime.getRuntime().availableProcessors());
//        String[] args = {"list://10.245.150.46:8061,10.245.150.46:8062,10.245.150.46:8063," +
//                "10.245.150.46:8064,10.245.150.46:8065,10.245.150.46:8066,10.245.150.46:8067"};
//        String[] args = {"list://10.245.150.46:8071,10.245.150.46:8072,10.245.150.46:8073"};
        String[] args = {"list://10.245.150.47:8071,10.245.150.47:8072,10.245.150.47:8073,10.245.150.47:8074,10.245.150.47:8075"+
        ",10.245.150.47:8061,10.245.150.47:8062,10.245.150.47:8063,10.245.150.47:8064,10.245.150.47:8065"+
        ",10.245.150.47:8051,10.245.150.47:8052,10.245.150.47:8053,10.245.150.47:8054,10.245.150.47:8055"+
        ",10.245.150.47:8041,10.245.150.47:8042,10.245.150.47:8043,10.245.150.47:8044,10.245.150.47:8045"+
        ",10.245.150.47:8031,10.245.150.47:8032,10.245.150.47:8033,10.245.150.47:8034,10.245.150.47:8035"};
        ConcurrentClientMain.main(args);
//        String[] args = {"list://127.0.0.1:8062,127.0.0.1:8063", "3dc140fd-24f1-42f4-8246-dfea8bd4005c"};
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
