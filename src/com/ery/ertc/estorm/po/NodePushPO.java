package com.ery.ertc.estorm.po;

import java.io.Serializable;

public class NodePushPO implements Serializable{

    private long PUSH_ID ;
    private long NODE_ID;
    private long PUSH_TYPE;//订阅类型，对应【POConstant.PUSH_TYPE_?】
    private String PUSH_URL ;
    private String PUSH_USER ;
    private String PUSH_PASS ;
    private String PUSH_EXPR ;
    private String PUSH_PARAM  ;

}
