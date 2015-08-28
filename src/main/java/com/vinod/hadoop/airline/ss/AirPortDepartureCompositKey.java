package com.vinod.hadoop.airline.ss;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class AirPortDepartureCompositKey implements WritableComparable<AirPortDepartureCompositKey>{

	private String airPort;
	private long depttime;
	
	public AirPortDepartureCompositKey() {
		
	}
	public AirPortDepartureCompositKey(String airPort,  long depttime) {
	  set(airPort,depttime);
	}
	
	public void set(String airPort,  long depttime) {
		this.airPort = airPort;
		
		this.depttime = depttime;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(airPort);
		
		
		out.writeLong(this.depttime);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.airPort = in.readUTF();
		
		
		this.depttime = in.readLong();
	
	}

	@Override
	public int compareTo(AirPortDepartureCompositKey o) {
		if(this.airPort.compareTo(o.getAirPort()) != 0) {
			return this.airPort.compareTo(o.getAirPort());
		}  else if(this.getDepttime() != o.getDepttime()) {
			return this.depttime < o.getDepttime() ? -1 : 1;
		} else {
			return 0;
		}
	}

	public String getAirPort() {
		return airPort;
	}

	public void setAirPort(String airPort) {
		this.airPort = airPort;
	}

	

	public long getDepttime() {
		return depttime;
	}

	public void setDepttime(long depttime) {
		this.depttime = depttime;
	}
	@Override
	public String toString() {
		return "AirPortDepartureCompositKey [airPort=" + airPort + ", depttime=" + depttime + "]";
	}
	
}

	
