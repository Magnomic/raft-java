package com.github.wenweihu86.raft.example.server.service.impl;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.wenweihu86.raft.Peer;
import com.github.wenweihu86.raft.example.server.ExampleStateMachine;
import com.github.wenweihu86.raft.example.server.service.ExampleProto;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wenweihu86 on 2017/5/9.
 */
public class ExampleServiceImpl implements ExampleService {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleServiceImpl.class);
    private static JsonFormat jsonFormat = new JsonFormat();

    private RaftNode raftNode;
    private ExampleStateMachine stateMachine;
    private int leaderId = -1;
    private RpcClient leaderRpcClient = null;
    private ExampleService exampleService;
    private Lock leaderLock = new ReentrantLock();

    public ExampleServiceImpl(RaftNode raftNode, ExampleStateMachine stateMachine) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
    }

    private void onLeaderChangeEvent() {
        // 如果有leader，且自己不是Leader，且更换了Leader
        if (raftNode.getLeaderId() != -1
                && raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()
                && leaderId != raftNode.getLeaderId()) {
            leaderLock.lock();
            if (leaderId != -1 && leaderRpcClient != null) {
                leaderRpcClient.stop();
                leaderRpcClient = null;
                leaderId = -1;
            }
            leaderId = raftNode.getLeaderId();
            LOG.info("My leader is {}", leaderId);
            Peer peer = raftNode.getPeerMap().get(leaderId);
            Endpoint endpoint = new Endpoint(peer.getServer().getEndpoint().getHost(),
                    peer.getServer().getEndpoint().getPort());
            RpcClientOptions rpcClientOptions = new RpcClientOptions();
            rpcClientOptions.setGlobalThreadPoolSharing(true);
            leaderRpcClient = new RpcClient(endpoint, rpcClientOptions);
            leaderLock.unlock();
        }
    }

    @Override
    public ExampleProto.SetResponse set(ExampleProto.SetRequest request) {
        ExampleProto.SetResponse.Builder responseBuilder = ExampleProto.SetResponse.newBuilder();
        // 如果没Leader
        if (request.getType().equals("N")){
            LOG.info("received Normal request!");
        } else {
            LOG.info("received Future requests!");
        }
        if (raftNode.getLeaderId() <= 0) {
            responseBuilder.setSuccess(false);
            // 如果自己不是leader，将写请求转发给leader
        } else if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
            // TODO: 如果是非关键事务，则发布未来事务（是否需要增加一致性哈希）
            // 检查是否Leader已更换
            if (!request.getType().equals("F")) {
                onLeaderChangeEvent();
                if (this.exampleService == null) {
                    this.exampleService = BrpcProxy.getProxy(leaderRpcClient, ExampleService.class);
                }
                ExampleProto.SetResponse responseFromLeader = exampleService.set(request);
                responseBuilder.mergeFrom(responseFromLeader);
            } else {
                LOG.info("Publish new future transaction");
                // 如果是Future data，向各节点发送数据追加请求
                byte[] data = request.toByteArray();
                boolean success = raftNode.replicate(data, RaftProto.EntryType.ENTRY_TYPE_FUTURE_DATA);
                responseBuilder.setSuccess(success);
            }
        } else {
            if (request.getType().equals("N")) {
                // 如果自己是Leader，数据同步写入raft集群
                byte[] data = request.toByteArray();
                boolean success = raftNode.replicate(data, RaftProto.EntryType.ENTRY_TYPE_DATA);
                responseBuilder.setSuccess(success);
            } else if (request.getType().equals("C")){
                // 如果自己是Leader，数据同步写入raft集群
                byte[] data = request.toByteArray();
                boolean success = raftNode.replicate(data, RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION);
                responseBuilder.setSuccess(success);
            }
        }

        ExampleProto.SetResponse response = responseBuilder.build();
        LOG.info("set request, request={}, response={}", jsonFormat.printToString(request),
                jsonFormat.printToString(response));
        return response;
    }

    @Override
    public ExampleProto.GetResponse get(ExampleProto.GetRequest request) {
        ExampleProto.GetResponse response = stateMachine.get(request);
        LOG.info("get request, request={}, response={}", jsonFormat.printToString(request),
                jsonFormat.printToString(response));
        return response;
    }

}
