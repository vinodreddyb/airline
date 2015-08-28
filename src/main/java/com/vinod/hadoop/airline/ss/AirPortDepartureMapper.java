package com.vinod.hadoop.airline.ss;

import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirPortDepartureMapper
		extends Mapper<LongWritable, Text, AirPortDepartureCompositKey, AirPortDepartureNaturalValue> {

	private AirPortDepartureCompositKey ckey = new AirPortDepartureCompositKey();
	private AirPortDepartureNaturalValue nvalue = new AirPortDepartureNaturalValue();
	private String header = "Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay";

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, AirPortDepartureCompositKey, AirPortDepartureNaturalValue>.Context context)
					throws IOException, InterruptedException {

		if (!header.equals(value.toString())) {
			String[] tokens = StringUtils.split(value.toString().trim(), ",");

			String airPort = tokens[16];
			String flight = tokens[9];

			String depTime = tokens[4];
			int hh = 0;
			int mm = 0;
			try {
				if (depTime.length() == 4) {
					hh = Integer.parseInt(depTime.substring(0, 2));
					mm = Integer.parseInt(depTime.substring(2, 4));
				} else if (depTime.length() == 3) {
					hh = Integer.parseInt(depTime.substring(0, 1));
					mm = Integer.parseInt(depTime.substring(1, 3));
				} else if (depTime.length() == 2) {
					hh = Integer.parseInt(depTime.substring(0, 2));
				}
			} catch (NumberFormatException e) {
				
			}
			Calendar departureDateTime = Calendar.getInstance();
			departureDateTime.set(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]) - 1,
					Integer.parseInt(tokens[2]), hh, mm);

			ckey.set(airPort, departureDateTime.getTimeInMillis());
			nvalue.set(departureDateTime.getTimeInMillis(), flight);
			System.out.println("KEY-->" + ckey);
			context.write(ckey, nvalue);

		}
	}

}
