package com.ery.ertc.collect.forwards;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import com.ery.base.support.utils.Convert;

public class FieldNumberPartitioner implements Partitioner {

	public FieldNumberPartitioner(VerifiableProperties verifiableProperties) {
	}

	/**
	 * 
	 * @param s
	 * @param i
	 * @return
	 */
	@Override
	public int partition(Object s, int i) {
		return (int) (Math.abs(Convert.toLong(s)) % i);
	}

}
