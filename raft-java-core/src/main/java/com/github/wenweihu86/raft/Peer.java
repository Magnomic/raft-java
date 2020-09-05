package com.github.wenweihu86.raft;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.service.RaftConsensusServiceAsync;

/**
 * Created by wenweihu86 on 2017/5/5.
 */
public class Peer {
    private RaftProto.Server server;
    private RpcClient rpcClient;
    private RaftConsensusServiceAsync raftConsensusServiceAsync;
    // 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long nextIndex;
    // 已复制日志的最高索引值
    private long matchIndex;
    // 需要发送给节点的下一条Future Entry的index，应当为发布节点的Node ID倍数
    private long nextFutureIndex;
    // 已经复制给节点的Future Entry的index，应当为发布节点的Node ID倍数
    private long matchFutureIndex;
    private volatile Boolean voteGranted;
    private volatile boolean isCatchUp;

    public Peer(RaftProto.Server server) {
        this.server = server;
        this.rpcClient = new RpcClient(new Endpoint(
                server.getEndpoint().getHost(),
                server.getEndpoint().getPort()));
        raftConsensusServiceAsync = BrpcProxy.getProxy(rpcClient, RaftConsensusServiceAsync.class);
        isCatchUp = false;
    }

    // 创建新的 RpcClient
    public RpcClient createClient() {
        return new RpcClient(new Endpoint(    // rpc 客户端
                server.getEndpoint().getHost(),
                server.getEndpoint().getPort()));   // 通过 RpcClient 构建 RaftConsensusServiceAsync 的代理类
    }

    public RaftProto.Server getServer() {
        return server;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public RaftConsensusServiceAsync getRaftConsensusServiceAsync() {
        return raftConsensusServiceAsync;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public Boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(Boolean voteGranted) {
        this.voteGranted = voteGranted;
    }


    public boolean isCatchUp() {
        return isCatchUp;
    }

    public void setCatchUp(boolean catchUp) {
        isCatchUp = catchUp;
    }

    public long getNextFutureIndex() {
        return nextFutureIndex;
    }

    public void setNextFutureIndex(long nextFutureIndex) {
        this.nextFutureIndex = nextFutureIndex;
    }

    public long getMatchFutureIndex() {
        return matchFutureIndex;
    }

    public void setMatchFutureIndex(long matchFutureIndex) {
        this.matchFutureIndex = matchFutureIndex;
    }
}
