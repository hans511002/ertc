package com.ery.ertc.estorm.po;

import java.io.Serializable;

public class LogPO implements Serializable{

    protected long LOG_ID;
    protected long MSG_ID;
    protected String SERVER_HOST;

    public long getLOG_ID() {
        return LOG_ID;
    }

    public void setLOG_ID(long LOG_ID) {
        this.LOG_ID = LOG_ID;
    }

    public long getMSG_ID() {
        return MSG_ID;
    }

    public void setMSG_ID(long MSG_ID) {
        this.MSG_ID = MSG_ID;
    }

    public String getSERVER_HOST() {
        return SERVER_HOST;
    }

    public void setSERVER_HOST(String SERVER_HOST) {
        this.SERVER_HOST = SERVER_HOST;
    }
}
