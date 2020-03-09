package javaproducer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.*;
import java.util.Properties;


public class avroproducer {

    final static String bootstrapServers = "3.6.250.221:9092";
    final static String zookeeperservers= "127.0.0.1:2181";
    public static void main(String[] args) {

        Properties properties = new Properties();



        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.setProperty("schema.registry.url", "http://localhost:8081");



         try {
            String mydrive = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://etl.cwbsstkppkvj.eu-west-1.rds.amazonaws.com:3306/etldb?user=admin&password=temp1234";
            Class.forName(mydrive);
            Connection con = DriverManager.getConnection(url);

           Statement stmt  =  con.createStatement();
           ResultSet rs = stmt.executeQuery("select * from raw_data");
           System.out.println(rs.toString());


           while(rs.next()) {
               System.out.println("the value is " + rs.getString(1));


               // create the producer
               KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(properties);



               String schemaString = "{\"namespace\": \"customerManagement.avro\",\"type\": \"record\", " +
                       "\"name\": \"Customer\"," +
                       "\"fields\": [" +
                       "{\"name\": \"id\", \"type\": \"int\"}," +
                       "{\"name\": \"client_name\", \"type\": \"string\"}," +
                       "{\"name\": \"client_code\", \"type\":\"string\"]" +
                       "{\"name\": \"Longitude\", \"type\":\"string\"]" +
                       "{\"name\": \"revenue\", \"type\":\"string\"]" +
                       "{\"name\": \"location\", \"type\":\"string\"]" +
                       "{\"name\": \"year\", \"type\":\"string\"]" +
                       "{\"name\": \"Sector\", \"type\":\"string\"]" +
                       "{\"name\": \"Major_business\", \"type\":\"string\"]" +
                       "{\"name\": \"nasdaq_site\", \"type\":\"string\"}" +
                       "]";


               Schema.Parser parser = new Schema.Parser();
               Schema schema = parser.parse(schemaString);

               GenericRecord transaction = new GenericData.Record(schema);

               transaction.put("id",rs.getString(1));
               transaction.put("client_name",rs.getString(2));
               transaction.put("client_code",rs.getString(3));
               transaction.put("Longitude",rs.getString(4));
               transaction.put("revenue",rs.getString(5));
               transaction.put("location",rs.getString(6));
               transaction.put("year",rs.getString(7));
               transaction.put("Sector",rs.getString(8));
               transaction.put("Major_business",rs.getString(9));
               transaction.put("nasdaq_site",rs.getString(10));
               // create a producer record
               ProducerRecord<String,GenericRecord> record =
                       new ProducerRecord<String, GenericRecord>
                               ("kstream",rs.getString(1), transaction);

               // send data - asynchronous
               producer.send(record);

               Thread.sleep(5000);

               // flush data
               //producer.flush();
               // flush and close producer
               producer.close();
           }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
             e.printStackTrace();
         }
    }
}

