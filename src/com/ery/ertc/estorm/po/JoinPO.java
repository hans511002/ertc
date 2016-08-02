package com.ery.ertc.estorm.po;

import java.io.Serializable;

public class JoinPO implements Serializable{

    private long JOIN_ID;
    private long DATA_SRC_ID;
    private String TABLE_NAME;
    private String  TABLE_SQL;
    private String DATA_QUERY_SQL;
    private int FLASH_TYPE;//刷新类型【POConstant.FLASH_TYPE_?】
    private String FLASH_PARAM;//周期：基准事件   间隔类型  间隔大小。事件：SQL事件,ZK监听，文件监听。（轮询时间框架参数配置）
    private String DATA_CHECK_RULE;//数据验证规则

    public long getJOIN_ID() {
        return JOIN_ID;
    }

    public void setJOIN_ID(long JOIN_ID) {
        this.JOIN_ID = JOIN_ID;
    }

    public long getDATA_SRC_ID() {
        return DATA_SRC_ID;
    }

    public void setDATA_SRC_ID(long DATA_SRC_ID) {
        this.DATA_SRC_ID = DATA_SRC_ID;
    }

    public String getTABLE_NAME() {
        return TABLE_NAME;
    }

    public void setTABLE_NAME(String TABLE_NAME) {
        this.TABLE_NAME = TABLE_NAME;
    }

    public String getTABLE_SQL() {
        return TABLE_SQL;
    }

    public void setTABLE_SQL(String TABLE_SQL) {
        this.TABLE_SQL = TABLE_SQL;
    }

    public String getDATA_QUERY_SQL() {
        return DATA_QUERY_SQL;
    }

    public void setDATA_QUERY_SQL(String DATA_QUERY_SQL) {
        this.DATA_QUERY_SQL = DATA_QUERY_SQL;
    }

    public int getFLASH_TYPE() {
        return FLASH_TYPE;
    }

    public void setFLASH_TYPE(int FLASH_TYPE) {
        this.FLASH_TYPE = FLASH_TYPE;
    }

    public String getFLASH_PARAM() {
        return FLASH_PARAM;
    }

    public void setFLASH_PARAM(String FLASH_PARAM) {
        this.FLASH_PARAM = FLASH_PARAM;
    }

    public String getDATA_CHECK_RULE() {
        return DATA_CHECK_RULE;
    }

    public void setDATA_CHECK_RULE(String DATA_CHECK_RULE) {
        this.DATA_CHECK_RULE = DATA_CHECK_RULE;
    }
}
