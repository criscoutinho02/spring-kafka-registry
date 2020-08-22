package com.sunilvb.demo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.sunilvb.demo.serializer.GenericSerializer;
import com.sunilvb.demo.serializer.OrderSerializer;

import generated.avro.com.sunilvb.demo.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

@SpringBootApplication
@RestController
public class SpringKafkaRegistryApplication {

	final static Logger logger = Logger.getLogger(SpringKafkaRegistryApplication.class);
	@Value("${bootstrap.url}")
	String bootstrap;
	@Value("${registry.url}")
	String registry;
	
	@RequestMapping("/orders")
	public String doIt(@RequestParam(value="name", defaultValue="Order-avro") String name)
	{
		
		String ret=name;
		try
		{
			ret += "<br>Using Bootstrap : " + bootstrap;
			ret += "<br>Using Bootstrap : " + registry;
			
			Properties properties = new Properties();
			// Kafka Properties
			properties.setProperty("bootstrap.servers", bootstrap);
			properties.setProperty("acks", "all");
			properties.setProperty("retries", "10");
			// Avro properties
			properties.setProperty("key.serializer", StringSerializer.class.getName());
			properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
			properties.setProperty("schema.registry.url", registry);
			
			ret += sendMsg(properties, name);
		}
		catch(Exception ex){ ret+="<br>"+ex.getMessage();}
		
		return ret;
	}
	
	private Order sendMsg(Properties properties, String topic)
	{
		Producer<String, Order> producer = new KafkaProducer<String, Order>(properties);

        Order order = Order.newBuilder()
        		.setOrderId("OId234")
        		.setCustomerId("CId432")
        		.setSupplierId("SId543")
                .setItems(4)
                .setFirstName("Sunil")
                .setLastName("V")
                .setPrice(178f)
                .setWeight(75f)
                .build();

        ProducerRecord<String, Order> producerRecord = new ProducerRecord<String, Order>(topic, order);

        
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    logger.info(metadata); 
                } else {
                	logger.error(exception.getMessage());
                }
            }
        });

        producer.flush();
        producer.close();
        
        return order;
	}
	
	
	public void gerarAvroSpecificRecord() throws IOException {
		
		List<Order> ordens = new ArrayList<Order>();


		Order order1 = Order.newBuilder()
	        		.setCustomerId("434567")
	        		.setSupplierId("oiyt")
	                .setItems(4)
	                .setFirstName("Cristina")
	                .setLastName("Coutinho")
	                .setPrice(178f)
	                .setWeight(75f)
	                .build();
		 Order order2 = Order.newBuilder()
	        		.setCustomerId("2")
	        		.setSupplierId("2345")
	                .setItems(4)
	                .setFirstName("Juliana")
	                .setLastName("Coutinho")
	                .setPrice(178f)
	                .setWeight(75f)
	                .build();
		 Order order3 = Order.newBuilder()
	        		.setCustomerId("888")
	        		.setSupplierId("oiyt")
	                .setItems(4)
	                .setFirstName("Agnelo")
	                .setLastName("Ferreira")
	                .setPrice(178f)
	                .setWeight(75f)
	                .setOrderId("345")
	                .build();
		 
		 ordens.add(order1);
		 ordens.add(order2);
		 ordens.add(order3);
		 
		 
		 
		 OrderSerializer serializer = new OrderSerializer();
		 List<Order> ordensSerializadas = new ArrayList<Order>();

		 ordensSerializadas = serializer.deserializarOrder("ordems.avro");
		 System.out.println(ordensSerializadas);
		 
		 String nome;
		 
	}
	
	public static void gerarAvroGenerico() {
		
		GenericSerializer generic = new GenericSerializer();
		try {
			generic.serializarGenerico("order.avsc", "orderGeneric.avro");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
	
	public static void lerAvroGenerico() {
		
		GenericSerializer generic = new GenericSerializer();
		
		List<GenericRecord> genericList = new ArrayList<GenericRecord>();
		try {
			genericList = generic.deserializarGenerico("order.avsc", "orderGeneric.avro");
			
			System.out.println(genericList);
			
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}
	
	public static void main(String[] args)  {
		SpringApplication.run(SpringKafkaRegistryApplication.class, args);
		
		gerarAvroGenerico();
		
		
		lerAvroGenerico();
	}
	
	
}
