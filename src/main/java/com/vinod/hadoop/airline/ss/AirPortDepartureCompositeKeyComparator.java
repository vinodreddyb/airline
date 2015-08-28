package com.vinod.hadoop.airline.ss;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



public class AirPortDepartureCompositeKeyComparator extends WritableComparator {

	public AirPortDepartureCompositeKeyComparator() {
		super(AirPortDepartureCompositKey.class, true);
	}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		AirPortDepartureCompositKey ck1 = (AirPortDepartureCompositKey) a;
		AirPortDepartureCompositKey ck2 = (AirPortDepartureCompositKey) b;
		int comparison = ck1.getAirPort().compareTo(ck2.getAirPort());
		if (comparison == 0) {
			if (ck1.getDepttime() == ck2.getDepttime()) {
				return 0;
			} else if (ck1.getDepttime() < ck2.getDepttime()) {
				return -1;
			} else {
				return 1;
			}

		} else {
			return comparison;
		}
		
	}
}
