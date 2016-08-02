package com.ery.ertc.estorm.po;

public class CollectErrorLogPO extends LogPO{
    
    private String ERROR_TIME;
    private String ERROR_INFO;
    private String SRC_CLIENT;

    public String getERROR_TIME() {
        return ERROR_TIME;
    }

    public void setERROR_TIME(String ERROR_TIME) {
        this.ERROR_TIME = ERROR_TIME;
    }

    public String getERROR_INFO() {
        return ERROR_INFO;
    }

    public void setERROR_INFO(String ERROR_INFO) {
        this.ERROR_INFO = ERROR_INFO;
    }

    public String getSRC_CLIENT() {
        return SRC_CLIENT;
    }

    public void setSRC_CLIENT(String SRC_CLIENT) {
        this.SRC_CLIENT = SRC_CLIENT;
    }
}
