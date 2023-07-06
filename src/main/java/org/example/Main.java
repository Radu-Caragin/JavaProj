package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] csv) {

        SparkSession sparkSession = SparkSession.builder().master("local").appName("Read_Erasmus").getOrCreate();
        Dataset<Row> dataset=  sparkSession.read().option("header","true").csv("E:/practica_IBM/JavaProj/src/main/resources/Erasmus.csv");
        dataset.show(15,false);

        }
    }
