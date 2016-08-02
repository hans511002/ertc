package com.ery.ertc.estorm.po;

import java.io.Serializable;

public class NodeStorePO implements Serializable{

    private long NODE_ID ;
    private long DATA_SRC_ID;
    private String TARGET_NAME;
    private String STORE_FIELD_MAPPING ;
    private String TARGET_PRIMARY_RULE;

    public long getNODE_ID() {
        return NODE_ID;
    }

    public void setNODE_ID(long NODE_ID) {
        this.NODE_ID = NODE_ID;
    }

    public long getDATA_SRC_ID() {
        return DATA_SRC_ID;
    }

    public void setDATA_SRC_ID(long DATA_SRC_ID) {
        this.DATA_SRC_ID = DATA_SRC_ID;
    }

    public String getTARGET_NAME() {
        return TARGET_NAME;
    }

    public void setTARGET_NAME(String TARGET_NAME) {
        this.TARGET_NAME = TARGET_NAME;
    }

    public String getSTORE_FIELD_MAPPING() {
        return STORE_FIELD_MAPPING;
    }

    public void setSTORE_FIELD_MAPPING(String STORE_FIELD_MAPPING) {
        this.STORE_FIELD_MAPPING = STORE_FIELD_MAPPING;
    }

    public String getTARGET_PRIMARY_RULE() {
        return TARGET_PRIMARY_RULE;
    }

    public void setTARGET_PRIMARY_RULE(String TARGET_PRIMARY_RULE) {
        this.TARGET_PRIMARY_RULE = TARGET_PRIMARY_RULE;
    }
}
