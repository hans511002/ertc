package com.ery.ertc.estorm.po;

import java.util.concurrent.atomic.AtomicLong;

public class CollectTotalLogPO extends LogPO{
    
    private String START_TIME;
    private String STOP_TIME;
    private AtomicLong TOTAL_NUM;
    private AtomicLong ERROR_NUM;
    private int STOP_FLAG;
    private AtomicLong SEND_NUM;
    private AtomicLong SEND_ERROR_NUM;

    public String getSTART_TIME() {
        return START_TIME;
    }

    public void setSTART_TIME(String START_TIME) {
        this.START_TIME = START_TIME;
    }

    public String getSTOP_TIME() {
        return STOP_TIME;
    }

    public void setSTOP_TIME(String STOP_TIME) {
        this.STOP_TIME = STOP_TIME;
    }

    public AtomicLong getTOTAL_NUM() {
        return TOTAL_NUM;
    }

    public void setTOTAL_NUM(AtomicLong TOTAL_NUM) {
        this.TOTAL_NUM = TOTAL_NUM;
    }

    public AtomicLong getERROR_NUM() {
        return ERROR_NUM;
    }

    public void setERROR_NUM(AtomicLong ERROR_NUM) {
        this.ERROR_NUM = ERROR_NUM;
    }

    public int getSTOP_FLAG() {
        return STOP_FLAG;
    }

    public void setSTOP_FLAG(int STOP_FLAG) {
        this.STOP_FLAG = STOP_FLAG;
    }

    public AtomicLong getSEND_NUM() {
        return SEND_NUM;
    }

    public void setSEND_NUM(AtomicLong SEND_NUM) {
        this.SEND_NUM = SEND_NUM;
    }

    public AtomicLong getSEND_ERROR_NUM() {
        return SEND_ERROR_NUM;
    }

    public void setSEND_ERROR_NUM(AtomicLong SEND_ERROR_NUM) {
        this.SEND_ERROR_NUM = SEND_ERROR_NUM;
    }
}
