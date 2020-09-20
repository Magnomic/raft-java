package com.github.wenweihu86.raft.example.client;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.github.wenweihu86.raft.example.server.service.ExampleProto;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.googlecode.protobuf.format.JsonFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ClientMain {
    public static void main(String[] args) {
//        System.out.println(Arrays.toString(args));
        if (args.length < 2) {
            System.out.print("Usage: ./run_client.sh CLUSTER KEY [VALUE]\n");
            System.exit(-1);
        }

        // parse args
        final String ipPorts = args[0];
        final String key = args[1];
        String value = null;
        if (args.length > 2) {
            value = args[2];
        }

        final String finalValue = value;

        // init rpc client
        RpcClient rpcClient = new RpcClient(ipPorts);
        ExampleService exampleService = BrpcProxy.getProxy(rpcClient, ExampleService.class);
        final JsonFormat jsonFormat = new JsonFormat();

//        // set
//        if (finalValue != null) {
//            // TODO: 是否为非关键事务，增加type属性
//            ExampleProto.SetRequest setRequest = ExampleProto.SetRequest.newBuilder()
//                    .setKey(key).setValue(finalValue).build();
//            ExampleProto.SetResponse setResponse = exampleService.set(setRequest);
//            System.out.printf("set request, key=%s value=%s response=%s\n",
//                    key, finalValue, jsonFormat.printToString(setResponse));
//
//        } else {
            // get
            ExampleProto.GetRequest getRequest = ExampleProto.GetRequest.newBuilder()
                    .setKey(key).build();
            ExampleProto.GetResponse getResponse = exampleService.get(getRequest);
            System.out.printf("get request, key=%s, response=%s\n",
                    key, jsonFormat.printToString(getResponse));
//        }
//        rpcClient.stop();
    }
}
