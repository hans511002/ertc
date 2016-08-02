package com.ery.ertc.collect.remote.client;

import com.ery.ertc.collect.worker.executor.AbstractTaskExecutor;
import com.ery.ertc.estorm.po.MsgPO;

public class HbaseCollectExec extends CollectExec {
	public HbaseCollectExec(AbstractTaskExecutor executor) {
		super(executor);
	}

	@Override
	public void execute() {
		MsgPO msgPO = executor.getMsgPO();
	}
}
