package com.ery.ertc.estorm.po;

import com.ery.base.support.utils.Convert;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class CollectLogPO extends LogPO{

    private String START_TIME;
    private String END_TIME;
    private AtomicLong BYTE_SIZE = new AtomicLong(0);
    private AtomicLong MSG_NUM = new AtomicLong(0);

    public CollectLogPO(long msgId,String serverName) {
        this.MSG_ID = msgId;
        this.SERVER_HOST = serverName;
        this.START_TIME = Convert.toTimeStr(new Date(),null);
    }

    public String getSTART_TIME() {
        return START_TIME;
    }

    public void setSTART_TIME(String START_TIME) {
        this.START_TIME = START_TIME;
    }

    public String getEND_TIME() {
        return END_TIME;
    }

    public void setEND_TIME(String END_TIME) {
        this.END_TIME = END_TIME;
    }

    public AtomicLong getBYTE_SIZE() {
        return BYTE_SIZE;
    }

    public void setBYTE_SIZE(AtomicLong BYTE_SIZE) {
        this.BYTE_SIZE = BYTE_SIZE;
    }

    public AtomicLong getMSG_NUM() {
        return MSG_NUM;
    }

    public void setMSG_NUM(AtomicLong MSG_NUM) {
        this.MSG_NUM = MSG_NUM;
    }
}
