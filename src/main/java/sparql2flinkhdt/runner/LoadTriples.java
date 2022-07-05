package sparql2flinkhdt.runner;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.apache.jena.graph.Triple;


public class LoadTriples {

    public static DataStream<Triple> fromDataset(StreamExecutionEnvironment environment, String filePath) {
        Preconditions.checkNotNull(filePath, "The file path may not be null.");

        DataStreamSource<String> dataStreamSource = environment.readTextFile(filePath);

        DataStream<Triple> datastream = dataStreamSource.map(new String2Triple());

        return datastream;
    }
}
