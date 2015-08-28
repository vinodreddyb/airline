package com.vinod.hadoop.airline.ss;



public class AirPortDepartureNaturalKeyPartitioner extends org.apache.hadoop.mapreduce.Partitioner<AirPortDepartureCompositKey, AirPortDepartureNaturalValue> {

	@Override
	public int getPartition(AirPortDepartureCompositKey key, AirPortDepartureNaturalValue value, int numberOfPartitions) {
		
		return Math.abs(key.getAirPort().hashCode() % numberOfPartitions);
	}
}
