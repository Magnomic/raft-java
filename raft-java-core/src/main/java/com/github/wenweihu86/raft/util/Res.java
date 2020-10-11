package com.github.wenweihu86.raft.util;

public class Res {
    private boolean success;
    private Long wait;
    private Long index;

    public Res() {
    }

    public Res(boolean success) {
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Long getWait() {
        return wait;
    }

    public void setWait(Long wait) {
        this.wait = wait;
    }

    public Long getIndex() {
        return index;
    }

    public void setIndex(Long index) {
        this.index = index;
    }
}
