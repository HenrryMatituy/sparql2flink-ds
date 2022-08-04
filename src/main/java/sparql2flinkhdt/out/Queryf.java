package sparql2flinkhdt.out;

//import org.apache.flink.streaming.api.scala.*;
//import org.apache.flink.streaming.api.scala.extensions._;

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.jena.graph.Triple;
import sparql2flinkhdt.runner.functions.*;
import sparql2flinkhdt.runner.LoadTriples;

import org.apache.flink.api.common.RuntimeExecutionMode;

public class Queryf {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        if (!params.has("dataset") && !params.has("output")) {
            System.out.println("Use --dataset to specify dataset path and use --output to specify output path.");
        }

        //************ Environment (DataSet) and Source (static RDF dataset) ************
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<Triple> datastream = LoadTriples.fromDataset(env, params.get("dataset"));

        //************ Applying Transformations ************
        DataStream<SolutionMapping> sm1 = datastream
                .filter(new Triple2Triple(null, "http://xmlns.com/foaf/0.1/name", null))
                .map(new Triple2SolutionMapping("?person", null, "?name"));

        DataStream<SolutionMapping> sm2 = datastream
                .filter(new Triple2Triple(null, "http://xmlns.com/foaf/0.1/mbox", null))
                .map(new Triple2SolutionMapping("?person", null, "?mbox"));

//		DataStream<SolutionMapping> sm3 = sm1.leftOuterJoin(sm2)
//			.where(new JoinKeySelector(new String[]{"?person"}))
//			.equalTo(new JoinKeySelector(new String[]{"?person"}))
//			.with(new LeftJoin());

//         CoGroupedStreams.WithWindow<SolutionMapping, SolutionMapping, String, TimeWindow> sm3 = sm1.coGroup(sm2)
//                .where(new JoinKeySelector(new String[]{"?person"}))
//                .equalTo(new JoinKeySelector(new String[]{"?person"}))
//                .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)));

        DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
                .where(new JoinKeySelector(new String[]{"?person"}))
                .equalTo(new JoinKeySelector(new String[]{"?person"}))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply (new CoGroup());
//                .apply(new CoGroupFunction<DataStream<SolutionMapping>, DataStream<SolutionMapping>, DataStream<SolutionMapping>>() {
//                    //@Override
//                    public void coGroup(Iterable<DataStream<SolutionMapping>> left, Iterable<DataStream<SolutionMapping>> right, Collector<DataStream<SolutionMapping>> collector) throws Exception {
//
//
//                        collector.collect(left,right);
//                    }
//                })
//                .print();





//                  .window(TumblingEventTimeWindows.of(Time);


//                .apply(new MyCoGroupFunction());
//
//        sm1.coGroup(sm2)
//                .where(new JoinKeySelector(new String[]{"?person"}))
//                .equalTo(new JoinKeySelector(new String[]{"?person"}))
//                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(30)))
//                .apply(someCoGroupFunction)
//                .sinkTo(someSink);

//
//        stream1.coGroup(stream2)
//                .where(stream1Item -> streamItem.field1)
//                .equalTo(stream2Item -> stream2Item.field1)
//                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(30)))
//                .apply(someCoGroupFunction)
//                .sinkTo(someSink);
//
//        DataStream<Tuple2<String, Integer>> in = ...;
//        KeyedStream<Tuple2<String, Integer>, String> keyed = in.keyBy(...);
//        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowed =
//                keyed.window(TumblingEventTimeWindows.of(Time.minutes(1)));

//        sm1.coGroup(sm2)
//                .where(new JoinKeySelector(new String[]{"?person"}))
//                .equalTo(new JoinKeySelector(new String[]{"?person"}))
//                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(30)));

//        sm1.coGroup(sm2)
//                .where(new JoinKeySelector(new String[]{"?person"}))
//              .equalTo(new JoinKeySelector(new String[]{"?person"}))
//        .window(CoGroupedStreams<sm1>.Wi)
//        .apply(new MyCoGroupFunction());


//        sm1.coGroup(sm2)
//                .where(new JoinKeySelector(new String[]{"?person"}))
//                .equalTo(new JoinKeySelector(new String[]{"?person"}))
//                .window(TumblingEventTimeWindows.of(Time.minutes(Constants.)))
//                .evictor(TimeEvictor.of(Time.minutes(Constants.VALIDTY_WINDOW_MIN)))
//                .apply(new CalculateCoGroupFunction());


//				.window(TumblingEventTimeWindows.of(Time.milliseconds(1)));
//				.window( TumblingEventTimeWindows.of(Time.of(3, TimeUnit.SECONDS)),new TimeWindow.Serializer());
//				.window(TumblingEventTimeWindows.of(Time.seconds(3)));

//		CoGroupedStreams.WithWindow<SolutionMapping, SolutionMapping, String, TimeWindow> sm3 = sm1.coGroup(sm2)
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.window(TumblingEventTimeWindows.of(Time.seconds(3)));



        DataStream<SolutionMapping> sm4 = sm3
                .map(new Project(new String[]{"?person", "?name", "?mbox"}));
        //************ Sink  ************
        sm4.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("SPARQL Query to Flink Progran - DataStream API");
    }
}
