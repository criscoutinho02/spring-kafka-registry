package com.sunilvb.demo.serializer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericSerializer {

	
	public void serializarGenerico(String avroName , String fileName) throws IOException {
		
		Schema schema = new Schema.Parser().parse(new File(avroName));
		
		GenericRecord order1 = new GenericData.Record(schema);
		order1.put("customer_id", "1234");
		order1.put("supplier_id", "5678");
		order1.put("first_name", "Olimpia");
		order1.put("last_name", "Valelongo");
		order1.put("items", 5);
		order1.put("price", new Float(100.99));
		order1.put("weight", new Float(123));
		order1.put("automated_email", true);
		
		GenericRecord order2 = new GenericData.Record(schema);
		order2.put("customer_id", "1234");
		order2.put("supplier_id", "5678");
		order2.put("first_name", "Cris");
		order2.put("last_name", "Valelongo");
		order2.put("items", 7);
		order2.put("price", new Float(10.90));
		order2.put("weight", new Float(234));
		order2.put("automated_email", false);
		
		File file = new File(fileName);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(order1);
		dataFileWriter.append(order2);
		dataFileWriter.close();
		
		
		
	}
	
	public List<GenericRecord> deserializarGenerico(String avroName , String fileName){
		
		Schema schema;
		try {
			schema = new Schema.Parser().parse(new File(avroName));
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(fileName), datumReader);
			
			List<GenericRecord> orders = new ArrayList<GenericRecord>();
			GenericRecord order = null;
			while(dataFileReader.hasNext()) {
				orders.add(dataFileReader.next());
			}
			
			return orders;
			
		
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
}
