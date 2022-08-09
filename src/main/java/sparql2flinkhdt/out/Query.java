
package sparql2flinkhdt.out;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import sparql2flinkhdt.runner.functions.*;
import sparql2flinkhdt.runner.LoadTriples;
import sparql2flinkhdt.runner.functions.order.*;

import java.math.*;

import org.apache.flink.api.common.RuntimeExecutionMode;

public class Query {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        if (!params.has("dataset") && !params.has("output")) {
            System.out.println("Use --dataset to specify dataset path and use --output to specify output path.");
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<Triple> datastream = LoadTriples.fromDataset(env, params.get("dataset"));

        DataStream<SolutionMapping> sm1 = datastream
                .filter(new Triple2Triple(null, "http://xmlns.com/foaf/0.1/name", null))
                .map(new Triple2SolutionMapping("?person", null, "?name"));

        DataStream<SolutionMapping> sm2 = datastream
                .filter(new Triple2Triple(null, "http://xmlns.com/foaf/0.1/mbox", null))
                .map(new Triple2SolutionMapping("?person", null, "?mbox"));


        DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
                .where(new JoinKeySelector(new String[]{"?person"}))
                .equalTo(new JoinKeySelector(new String[]{"?person"}))
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(5))
                .apply(new CoGroup());

        DataStream<SolutionMapping> sm4 = sm3
                .map(new Project(new String[]{"?person", "?name", "?mbox"}));

        //************ Sink  ************
        sm4.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("SPARQL Query to Flink Progran - DataStream API");
    }
}
