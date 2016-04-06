package com.prabhakar.spark.example;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;

import scala.Tuple2;
import scala.Tuple4;

public class ExampleAppFunctions {

    private static final Logger logger = LoggerFactory.getLogger(ExampleAppFunctions.class);

    public static class ExtractPairFromEvents implements 
        PairFlatMapFunction < SparkFlumeEvent, Tuple2 < String, String > ,
        Tuple4 < Integer, Integer, Integer, Integer >> {

        private static final long serialVersionUID = 1L;

        private int[] parseQci(String strQciArray) {
            int[] qci = new int[12];
            String[] s = strQciArray.split(",", -1);
            int qciIndex = 0;
            for (int i = 1; i < s.length; i++) {
                if (i % 2 == 1) {
                    qciIndex = Integer.valueOf(s[i]);
                } else {
                    qci[qciIndex] = Integer.valueOf(s[i]);
                }
            }
            return qci;
        }

        @Override
        public Iterable < Tuple2 < Tuple2 < String, String > , 
        Tuple4 < Integer, Integer, Integer, Integer >>> call(SparkFlumeEvent event)
        throws Exception {
            List < Tuple2 < Tuple2 < String, String > , Tuple4 < Integer, Integer, Integer, Integer >>> 
                rstList = Lists.newArrayList();

            try {
                String body = new String(event.event().getBody().array(), Charset.forName("UTF-8"));
                String timestamp = "0";
                for (CharSequence cs: event.event().getHeaders().keySet()) {
                    if (cs.toString().equals("timestamp")) {
                        timestamp = event.event().getHeaders().get(cs).toString();
                    }
                }

                String[] cols = body.split("\\|", -1);
                if (cols.length > 514) {
                    logger.trace("MSG: {}|{}|{}|{}|{}|{}", 
                        cols[0], cols[507], cols[511], cols[509], cols[518], cols[514]);
                    int[] ExampleAbnormalEnbActQci = parseQci(cols[507]);
                    int[] ExampleAbnormalMmeActQci = parseQci(cols[511]);
                    int[] ExampleAbnormalEnbQci = parseQci(cols[509]);
                    int[] ExampleNormalEnbQci = parseQci(cols[518]);
                    int[] ExampleMmeQci = parseQci(cols[514]);

                    Tuple2 < String, String > key = new Tuple2 < String, String > (timestamp, cols[0]);
                    Tuple4 < Integer, Integer, Integer, Integer > val = 
                        new Tuple4 < Integer, Integer, Integer, Integer > 
                        (ExampleAbnormalEnbActQci[1] + ExampleAbnormalMmeActQci[1],
                    ExampleAbnormalEnbQci[1] + ExampleNormalEnbQci[1] + ExampleMmeQci[1],
                    ExampleAbnormalEnbActQci[8] + ExampleAbnormalMmeActQci[8],
                    ExampleAbnormalEnbQci[8] + ExampleNormalEnbQci[8] + ExampleMmeQci[8]);
                    logger.trace("{} qci: {},{},{},{}", cols[0], val._1(), val._2(), val._3(), val._4());
                    rstList.add(new Tuple2 < Tuple2 < String, String > , 
                        Tuple4 < Integer, Integer, Integer, Integer >> (key, val));
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
            return rstList;
        }

    }

    public static class ReduceCellByNode implements Function2 
    < Tuple4 < Integer, Integer, Integer, Integer > , Tuple4 < Integer, Integer, Integer, Integer > ,
    Tuple4 < Integer, Integer, Integer, Integer >> {

        private static final long serialVersionUID = 42L;


        @Override
        public Tuple4 < Integer, Integer, Integer, Integer > call(
        Tuple4 < Integer, Integer, Integer, Integer > v1,
        Tuple4 < Integer, Integer, Integer, Integer > v2) throws Exception {
            return new Tuple4 < Integer, Integer, Integer, Integer > (
            v1._1() + v2._1(),
            v1._2() + v2._2(),
            v1._3() + v2._3(),
            v1._4() + v2._4());
        }

    }

    public static class OutputNode implements Function < JavaPairRDD < Tuple2 < String, String > ,
    Tuple4 < Integer, Integer, Integer, Integer >> , Void > {

        private static final long serialVersionUID = 42L;

        @Override
        public Void call(
        JavaPairRDD < Tuple2 < String, String > , Tuple4 < Integer, Integer, Integer, Integer >> v1)
        throws Exception {
            Injector inj = Guice.createInjector(new ExampleInjector());
            ExampleOutputService service = inj.getInstance(ExampleOutputService.class);
            service.outputNode(v1.collect());
            return null;
        }

    }

}