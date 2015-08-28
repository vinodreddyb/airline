package com.vinod.hadoop.airline.ss;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirPortDepartureNaturalKeyGroupingComparator extends WritableComparator {

	public AirPortDepartureNaturalKeyGroupingComparator() {
		super(AirPortDepartureCompositKey.class, true);
	}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		AirPortDepartureCompositKey ck1 = (AirPortDepartureCompositKey) a;
		AirPortDepartureCompositKey ck2 = (AirPortDepartureCompositKey) b;
		
		return ck1.getAirPort().compareTo(ck2.getAirPort());
	}
}
