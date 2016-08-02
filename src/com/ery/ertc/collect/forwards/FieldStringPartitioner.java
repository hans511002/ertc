package com.ery.ertc.collect.forwards;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class FieldStringPartitioner implements Partitioner {

	public FieldStringPartitioner(VerifiableProperties verifiableProperties) {
	}

	@Override
	public int partition(Object s, int i) {
		return Math.abs(s.hashCode()) % i;
	}

}
