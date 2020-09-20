package com.github.wenweihu86.raft.example.client;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.github.wenweihu86.raft.example.server.service.ExampleProto;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.googlecode.protobuf.format.JsonFormat;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by wenweihu86 on 2017/5/14.
 */

@Getter
@Setter
class Record {
    String key;
    String type;

    Record(String key, String type){
        this.key = key;
        this.type = type;
    }
}

public class ConcurrentClientMain {
    private static JsonFormat jsonFormat = new JsonFormat();



    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            System.out.printf("Usage: ./run_concurrent_client.sh THREAD_NUM\n");
            System.exit(-1);
        }

        // parse args
        String ipPorts = args[0];
        RpcClient rpcClient = new RpcClient(ipPorts);
        ExampleService exampleService = BrpcProxy.getProxy(rpcClient, ExampleService.class);

        ExecutorService readThreadPool = Executors.newFixedThreadPool(20);
        ExecutorService writeThreadPool = Executors.newFixedThreadPool(20);
        Future<?>[] future = new Future[15];
        // 每个线程提交的entry数量
        Integer submitSum = 1000;
        // 线程数
        Integer threadSum = 5;
        List<Record> queryArr = new ArrayList<>();
        // 预先生成 submitSum * threadSum 的 键值对， N代表普通请求， F代表无事务依赖请求，
        for (int j=0;j<submitSum*threadSum;j++){
            queryArr.add(new Record(UUID.randomUUID().toString(), j % 4 == 0? "N": "N"));
        }
        // 创建线程，开始提交entry
        for (int i = 0; i < threadSum; i++) {
            future[i] = writeThreadPool.submit(new SetTask(exampleService, readThreadPool, "F",
                    queryArr.subList(i*submitSum, (i+1)*submitSum), submitSum));
        }
        // 等待上面的线程执行完
        Thread.sleep(10000L);

        // 再来一次
        List<Record> queryArr2 = new ArrayList<>();
        for (int j=0;j<submitSum*threadSum;j++){
            queryArr2.add(new Record(UUID.randomUUID().toString(), j % 4 == 0? "N": "N"));
        }
        for (int i = 0; i < threadSum; i++) {
            future[i] = writeThreadPool.submit(new SetTask(exampleService, readThreadPool, "F",
                    queryArr2.subList(i*submitSum, (i+1)*submitSum), submitSum));
        }
        Thread.sleep(10000L);

        // 取，验证过程
        for (Record key: queryArr){
            readThreadPool.submit(new GetTask(exampleService, key.getKey(), key.getType()));
        }
        Thread.sleep(10000L);
//        Thread.sleep(2000L);
//        for (int i = 0; i < 15; i++) {
//            future[i] = writeThreadPool.submit(new SetTask(exampleService, readThreadPool, "N"));
//        }
    }

    public static class SetTask implements Runnable {
        private ExampleService exampleService;
        ExecutorService readThreadPool;
        private String type;
        private List<Record> queryArr;
        Integer submitSum;

        public SetTask(ExampleService exampleService, ExecutorService readThreadPool, String type,
                       List<Record> queryArr, Integer submitSum) {
            this.exampleService = exampleService;
            this.readThreadPool = readThreadPool;
            this.type = type;
            this.queryArr = queryArr;
            this.submitSum = submitSum;
        }

        @Override
        public void run() {
//            for (int i=0;i<10;i++) {
            for (int i=0;i<submitSum;i++) {
                ExampleProto.SetRequest setRequest = ExampleProto.SetRequest.newBuilder()
                        .setKey(queryArr.get(i).getKey()).setValue(queryArr.get(i).getKey())
                        .setType(queryArr.get(i).getType()).build();
//                        .setKey(key).setValue(value).setType(i % 100 == 0? "F": "F").build();

                long startTime = System.currentTimeMillis();
                ExampleProto.SetResponse setResponse = exampleService.set(setRequest);
                try {
                    if (setResponse != null) {
                        System.out.printf("set %s request, key=%s, value=%s, response=%s, elapseMS=%d\n",
                                setRequest.getType(), queryArr.get(i).getKey(), queryArr.get(i).getKey(), jsonFormat.printToString(setResponse), System.currentTimeMillis() - startTime);
//                        readThreadPool.submit(new GetTask(exampleService, key, setRequest.getType()));
                    } else {
                        System.out.printf("set request failed, key=%s value=%s\n", queryArr.get(i), queryArr.get(i));
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public static class GetTask implements Runnable {
        private ExampleService exampleService;
        private String key;
        private String type;

        public GetTask(ExampleService exampleService, String key, String type) {
            this.exampleService = exampleService;
            this.key = key;
            this.type = type;
        }

        @Override
        public void run() {
//            try {
//                Thread.sleep(5000L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            ExampleProto.GetRequest getRequest = ExampleProto.GetRequest.newBuilder()
                    .setKey(key).build();
//            while (true) {
            long startTime = System.currentTimeMillis();
            ExampleProto.GetResponse getResponse = exampleService.get(getRequest);
            try {
                if (getResponse != null) {
                    System.out.printf("get %s request, key=%s, response=%s, elapseMS=%d\n",
                            type, key, jsonFormat.printToString(getResponse), System.currentTimeMillis() - startTime);
                } else {
                    System.out.printf("get request failed, key=%s\n", key);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
//            }
        }
    }

}
