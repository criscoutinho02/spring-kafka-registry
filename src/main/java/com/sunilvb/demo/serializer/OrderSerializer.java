package com.sunilvb.demo.serializer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import generated.avro.com.sunilvb.demo.Order;

public class OrderSerializer {


	public void serializarOrdems(List<Order> orders) throws IOException {

		if(!orders.isEmpty()) {

			DatumWriter<Order> orderDatumWriter = new SpecificDatumWriter<Order>(Order.class);
			DataFileWriter<Order> dataFileWriter = new DataFileWriter<Order>(orderDatumWriter);

			Order ordem = orders.get(0);

			dataFileWriter.create(new Order().getSchema(), new File("ordems.avro"));

				orders.stream().forEach(
						(order) -> {
							try {
								dataFileWriter.append(order);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}	        
						);

			
				
			dataFileWriter.close();



		}
	}
	
	
	public List<Order> deserializarOrder(String fileName) throws IOException{
		DatumReader<Order> orderDatumReader = new SpecificDatumReader<Order> (Order.class);
		DataFileReader<Order> dataFileReader = new DataFileReader<Order>(new File(fileName), orderDatumReader);
		List<Order> ordems=  new ArrayList<>();
		while(dataFileReader.hasNext()) {
			ordems.add(dataFileReader.next());
		}
		return ordems;
		


	}

}
