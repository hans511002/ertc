package com.ery.ertc.estorm.po;

import java.io.Serializable;

public class NodeJoinPO implements Serializable{

    private long JOIN_ID;
    private long NODE_ID;
    /**
     * 输入字段列表
     如果PAR_JOIN_ID为0，标示自来来自节点，格式如下：
     INPUT_ID1.field1，INPUT_ID2.field2,
     如果PAR_JOIN_ID非0，标示上一个JOIN表的字段,格式如下：
     field1，field2
     */
    private String INPUT_FIELD;
    private String JOIN_TBALE_FIELD;
    private long PAR_JOIN_ID;

    public long getJOIN_ID() {
        return JOIN_ID;
    }

    public void setJOIN_ID(long JOIN_ID) {
        this.JOIN_ID = JOIN_ID;
    }

    public long getNODE_ID() {
        return NODE_ID;
    }

    public void setNODE_ID(long NODE_ID) {
        this.NODE_ID = NODE_ID;
    }

    public String getINPUT_FIELD() {
        return INPUT_FIELD;
    }

    public void setINPUT_FIELD(String INPUT_FIELD) {
        this.INPUT_FIELD = INPUT_FIELD;
    }

    public String getJOIN_TBALE_FIELD() {
        return JOIN_TBALE_FIELD;
    }

    public void setJOIN_TBALE_FIELD(String JOIN_TBALE_FIELD) {
        this.JOIN_TBALE_FIELD = JOIN_TBALE_FIELD;
    }

    public long getPAR_JOIN_ID() {
        return PAR_JOIN_ID;
    }

    public void setPAR_JOIN_ID(long PAR_JOIN_ID) {
        this.PAR_JOIN_ID = PAR_JOIN_ID;
    }
}
