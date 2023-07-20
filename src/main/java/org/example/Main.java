package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Properties;


// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] csv) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Read Excel")
                .master("local")
                .getOrCreate();

        SparkSession sparkSession = SparkSession.builder().master("local").appName("Read_Erasmus").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("E:/practica_IBM/JavaProj/src/main/resources/Erasmus.csv");
        // dataset.show(15,false);


        //tema2
        dataset.printSchema();
        dataset.select("Receiving Country Code", "Sending Country Code").show(20, false);
        dataset.groupBy("Receiving Country Code", "Sending Country Code").count().withColumnRenamed("count", "Number of students").show();

    /*    Properties prop = new Properties();
        prop.setProperty("Username", "root");
        prop.setProperty("Password", "root");

        dataset.groupBy("Receiving Country Code", "Sending Country Code")
                .count()
                .orderBy("Receiving Country Code", "Sending Country Code")
                .write()
                .jdbc("jdbc:mysql://localhost:3306/erasmus", "tab", prop);*/

        dataBase(dataset,"ES","Estonia");
        dataBase(dataset,"FR","Franta");
        dataBase(dataset,"EL","Elvetia");


    }

    public static void dataBase(Dataset<Row> dataset, String countryCode, String tableName) {
        dataset
                .filter(dataset.col("Receiving Country Code").isin(countryCode))
                .groupBy("Receiving Country Code", "Sending Country Code")
                .count().orderBy("Receiving Country Code", "Sending Country Code")
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/ibm_erasmus")
                .option("dbtable", tableName)
                .option("user", "root")
                .option("password", "root")
                .save(tableName + ".erasmus");
    }
}
