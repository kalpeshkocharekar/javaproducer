package javaproducer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.sql.*;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;


public class sqlproducer {

    final static String bootstrapServers = "127.0.0.1:9092";
    final static String zookeeperservers= "127.0.0.1:2181";
    public static void main(String[] args) {

        Properties properties = new Properties();



        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


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
               KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
               ObjectNode transaction = JsonNodeFactory.instance.objectNode();

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
               ProducerRecord<String,String> record =
                       new ProducerRecord<String, String>
                               ("kstream",rs.getString(1), transaction.toString());

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

