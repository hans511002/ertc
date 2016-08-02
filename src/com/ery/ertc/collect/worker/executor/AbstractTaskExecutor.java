package com.ery.ertc.collect.worker.executor;

import com.ery.ertc.collect.worker.WorkerService;
import com.ery.ertc.estorm.po.MsgPO;

import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractTaskExecutor {
    protected MsgPO msgPO;
    protected WorkerService workerService;
    public abstract void start() throws Exception;
    public abstract void stop() throws Exception;
    public abstract boolean isRunning();

    public MsgPO getMsgPO() {
        return msgPO;
    }

    public void setMsgPO(MsgPO msgPO) {
        this.msgPO = msgPO;
    }
}
