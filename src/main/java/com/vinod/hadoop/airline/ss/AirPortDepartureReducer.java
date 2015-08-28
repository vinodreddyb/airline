package com.vinod.hadoop.airline.ss;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirPortDepartureReducer extends Reducer<AirPortDepartureCompositKey, AirPortDepartureNaturalValue, Text, Text>{

	private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm");
	@Override
	protected void reduce(AirPortDepartureCompositKey key, Iterable<AirPortDepartureNaturalValue> values,
			Reducer<AirPortDepartureCompositKey, AirPortDepartureNaturalValue, Text, Text>.Context context)
					throws IOException, InterruptedException {
		 StringBuilder builder = new StringBuilder();
		 System.out.println("REDUCE-->" + key);
		 for(AirPortDepartureNaturalValue value : values) {
			 System.out.println("Value-->" + value);
			 builder.append(" ");
			 builder.append(df.format(new Date(value.getDeptTime())));
			 builder.append(" ");
			 builder.append(value.getFlight());
			 builder.append("\n");
		 }
		 
		 context.write(new Text(key.getAirPort()), new Text(builder.toString()));
	}
}
