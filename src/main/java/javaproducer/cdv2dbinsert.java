package javaproducer;

import org.apache.kafka.streams.StreamsConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.io.*;
import java.sql.*;

public class cdv2dbinsert {




    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://etl.cwbsstkppkvj.eu-west-1.rds.amazonaws.com:3306/etldb";
        String username = "admin";
        String password = "temp1234";

        String csvFilePath = "C:\\Users\\KALPESH\\Downloads\\Ex_Files_Data_Eng_EssT\\Ex_Files_Data_Eng_EssT\\Exercise Files\\data\\clients.csv";

        int batchSize = 20;

        Connection connection = null;

        try {

            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO raw_data  VALUES (?,?, ?, ?, ?, ?,?,?,?,?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;

            int count = 0;

            lineReader.readLine(); // skip header line

            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                String id = data[0];
                String client_name = data[1];
                String client_code = data[2];
                String Longitude = data[3];
                String revenue = data[4];
                String location = data[5];
                String year = data[6];
                String Sector= data[7];
                 String Major_business=data[8];
                String nasdaq_site=data[9];

                statement.setString(1,id);
                statement.setString(2,client_name);
                statement.setString(3,client_code);
                statement.setString(4,Longitude);
                statement.setString(5,revenue);
                statement.setString(6,location);
                statement.setString(7,year);
                statement.setString(8,Sector);
                statement.setString(9,Major_business);
                statement.setString(10,nasdaq_site);
                Thread.sleep(5000);
                System.out.println(nasdaq_site);
                statement.addBatch();

                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            }

            lineReader.close();

            // execute the remaining queries
            statement.executeBatch();

            connection.commit();
            connection.close();

        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
