package com.ery.ertc.estorm.po;

import java.io.Serializable;

public class NodeInputPO implements Serializable{

    private long INPUT_ID;
    private int INPUT_TYPE;//节点输入类型，对应【POConstant.INPUT_TYPE_?】
    private long NODE_ID;//
    private long INPUT_SRC_ID;//inputType不同而表示不同的含义
    private String FILTER_EXPR;//过滤表达式

    public long getINPUT_ID() {
        return INPUT_ID;
    }

    public void setINPUT_ID(long INPUT_ID) {
        this.INPUT_ID = INPUT_ID;
    }

    public int getINPUT_TYPE() {
        return INPUT_TYPE;
    }

    public void setINPUT_TYPE(int INPUT_TYPE) {
        this.INPUT_TYPE = INPUT_TYPE;
    }

    public long getNODE_ID() {
        return NODE_ID;
    }

    public void setNODE_ID(long NODE_ID) {
        this.NODE_ID = NODE_ID;
    }

    public long getINPUT_SRC_ID() {
        return INPUT_SRC_ID;
    }

    public void setINPUT_SRC_ID(long INPUT_SRC_ID) {
        this.INPUT_SRC_ID = INPUT_SRC_ID;
    }

    public String getFILTER_EXPR() {
        return FILTER_EXPR;
    }

    public void setFILTER_EXPR(String FILTER_EXPR) {
        this.FILTER_EXPR = FILTER_EXPR;
    }
}
