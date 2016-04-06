package com.prabhakar.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import scala.Tuple2;
import scala.Tuple4;

public class ExampleApp {

    private static final Logger logger = LoggerFactory.getLogger(ExampleApp.class);

    @Parameter(names = "-host", variableArity = true, 
        description = "destination of the flume data", required = false)
    String host = "localhost"; 

    @Parameter(names = "-port", variableArity = true, 
        description = "Port of the destination", required = false)
    int port = 4445;

    @Parameter(names = "-uiport", variableArity = true, description = "port of spark UI", required = false)
    String uiport = "4041";

    public static void main(String[] args) {

        ExampleApp app = new ExampleApp();
        JCommander jc = new JCommander(app);
        try {
            jc.parse(args);
        } catch (Exception e) {
            logger.error(e.getMessage());
            jc.usage();
            return;
        }


        SparkConf conf = new SparkConf().setAppName("ExampleApp").set("spark.ui.port", app.uiport);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(10 * 1000));
        ssc.checkpoint("hdfs://HDFS_HOST:HDFS_PORT/spark/checkpoints/ExampleApp/");

        JavaReceiverInputDStream < SparkFlumeEvent > flumeStream = FlumeUtils.createStream(ssc,
        app.host, app.port, StorageLevel.MEMORY_ONLY());

        JavaPairDStream < Tuple2 < String, String > , Tuple4 < Integer, Integer, Integer, Integer >> 
            sampleStream = flumeStream.flatMapToPair(new ExampleAppFunctions.ExtractPairFromEvents())
            .reduceByKey(new ExampleAppFunctions.ReduceCellByNode());

        sampleStream.foreachRDD(new ExampleAppFunctions.OutputNode());

        ssc.start();
        ssc.awaitTermination();
    }
}