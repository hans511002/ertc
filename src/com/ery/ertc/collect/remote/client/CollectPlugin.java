package com.ery.ertc.collect.remote.client;

import com.ery.ertc.estorm.po.MsgPO;

public abstract class CollectPlugin {

    protected MsgPO msgPO;

    public void setMsgPO(MsgPO msgPO) {
        this.msgPO = msgPO;
    }

    public abstract void call();//执行

    public abstract void destroy();//资源释放

}
