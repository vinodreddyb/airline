package com.vinod.hadoop.airline.ss;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AirPortDepartureNaturalValue implements Writable, Comparable<AirPortDepartureNaturalValue>{


	private long deptTime;
	private String flight;
	
	public AirPortDepartureNaturalValue() {
		// TODO Auto-generated constructor stub
	}
	
	public AirPortDepartureNaturalValue(long deptTime, String flight) {
		set(deptTime, flight);
	}
	
	public void set(long deptTime, String flight) {
		this.deptTime = deptTime;
		this.flight = flight;
	}

	@Override
	public int compareTo(AirPortDepartureNaturalValue o) {
	
			if(this.deptTime == o.getDeptTime()) {
				return 0;
			} else if(this.deptTime < o.getDeptTime()) {
				return -1;
			} else {
				return 1;
			}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(this.deptTime);
		out.writeUTF(this.flight);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.deptTime = in.readLong();
		this.flight = in.readUTF();
		
	}

	
	public long getDeptTime() {
		return deptTime;
	}

	public void setDeptTime(long deptTime) {
		this.deptTime = deptTime;
	}

	public String getFlight() {
		return flight;
	}

	public void setFlight(String flight) {
		this.flight = flight;
	}

	@Override
	public String toString() {
		return "AirPortDepartureNaturalValue [deptTime=" + deptTime + ", flight=" + flight + "]";
	}
	
	

}
