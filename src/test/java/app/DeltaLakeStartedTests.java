package app;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;

import org.apache.commons.io.IOUtils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class DeltaLakeStartedTests {
    private static Logger logger = LoggerFactory.getLogger(DeltaLakeStartedTests.class);

    @Test
    public void testDataLake() throws Exception
    {
        String digitsCsv = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("dataLake/digits.csv"),
                StandardCharsets.UTF_8);
        logger.info(digitsCsv);

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("AIPlatformDataLake")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "spark");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

        File file = new File(Thread.currentThread().getContextClassLoader().
                getResource("dataLake/digits.csv").toURI());
        //Dataset<Row> df = spark.readStream().csv(file.getPath());
        final Dataset<Row> load = spark.read().csv(file.getPath());
        load.show();
        MongoSpark.save(load, writeConfig);


        /*Dataset<Row> df = spark.read().
        df.show();
        MongoSpark.save(df, writeConfig);*/

        spark.close();
    }
}
