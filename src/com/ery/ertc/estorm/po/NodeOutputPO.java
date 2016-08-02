package com.ery.ertc.estorm.po;

public class NodeOutputPO extends FieldPO{

    private long NODE_ID;
    /**
     * 计算表达式，可以是
     * 单字段：field1
     * 计算表达式:{a} && {1.b}  (1表示inputId)
     * 类：com.ery.ertc.Test：field1,field2；
     */
    private String CALC_EXPR;
    private String INPUT_FIELD_MAPPING;
    private String GROUP_METHOD;

    public long getNODE_ID() {
        return NODE_ID;
    }

    public void setNODE_ID(long NODE_ID) {
        this.NODE_ID = NODE_ID;
    }

    public String getCALC_EXPR() {
        return CALC_EXPR;
    }

    public void setCALC_EXPR(String CALC_EXPR) {
        this.CALC_EXPR = CALC_EXPR;
    }

    public String getINPUT_FIELD_MAPPING() {
        return INPUT_FIELD_MAPPING;
    }

    public void setINPUT_FIELD_MAPPING(String INPUT_FIELD_MAPPING) {
        this.INPUT_FIELD_MAPPING = INPUT_FIELD_MAPPING;
    }

    public String getGROUP_METHOD() {
        return GROUP_METHOD;
    }

    public void setGROUP_METHOD(String GROUP_METHOD) {
        this.GROUP_METHOD = GROUP_METHOD;
    }
}
