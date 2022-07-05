package sparql2flinkhdt.mapper;

import org.apache.jena.sparql.algebra.Op;
import java.nio.file.Path;

public class LogicalQueryPlan2FlinkProgram {

    private Op logicalQueryPlan;
    private String className;

    public LogicalQueryPlan2FlinkProgram(Op logicalQueryPlan, Path path){
        this.logicalQueryPlan = logicalQueryPlan;
        this.className = path.getFileName().toString();
        this.className = this.className.substring(0, this.className.indexOf('.'));
        this.className = this.className.toLowerCase();
        this.className = this.className.substring(0, 1).toUpperCase() + this.className.substring(1, this.className.length());
    }

    public String logicalQueryPlan2FlinkProgram() {
        String flinkProgram = "";

        flinkProgram += "package sparql2flinkhdt.out;\n\n" +
                "import org.apache.flink.api.java.DataSet;\n" +
                "import org.apache.flink.api.common.operators.Order;\n" +
                "import org.apache.flink.api.java.ExecutionEnvironment;\n" +
                "import org.apache.flink.api.java.utils.ParameterTool;\n" +
                "import org.apache.flink.core.fs.FileSystem;\n" +
                "import org.apache.flink.streaming.api.datastream.DataStream;\n" +
                "import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;\n" +
                "import org.apache.jena.graph.Node;\n" +
                "import org.apache.jena.graph.Triple;\n" +
                "import sparql2flinkhdt.runner.functions.*;\n" +
                "import sparql2flinkhdt.runner.LoadTriples;\n" +
                "import sparql2flinkhdt.runner.functions.order.*;\n" +
                "import java.math.*;\n" +
                "import org.apache.flink.api.common.RuntimeExecutionMode;\n" +
                "\npublic class " + className + " {\n" +
                "\tpublic static void main(String[] args) throws Exception {\n\n" +
                "\t\tfinal ParameterTool params = ParameterTool.fromArgs(args);\n\n" +
                "\t\tif (!params.has(\"dataset\") && !params.has(\"output\")) {\n" +
                "\t\t\tSystem.out.println(\"Use --dataset to specify dataset path and use --output to specify output path.\");\n" +
                "\t\t}\n\n" +
                "\t\t//************ Environment (DataSet) and Source (static RDF dataset) ************\n" +
                "\t\tfinal StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();\n" +
                "\t\tenv.setRuntimeMode(RuntimeExecutionMode.BATCH);\n" +
                "\t\tDataStream<Triple> datastream = LoadTriples.fromDataset(env, params.get(\"dataset\"));\n\n" +
                "\t\t//************ Applying Transformations ************\n";

        logicalQueryPlan.visit(new ConvertLQP2FlinkProgram());

        flinkProgram += ConvertLQP2FlinkProgram.getFlinkProgram();

        flinkProgram += "\t\t//************ Sink  ************\n" +
                "\t\tsm" + (SolutionMapping.getIndice() - 1) +
                ".writeAsText(params.get(\"output\")+\"" + className + "-Flink-Result\", FileSystem.WriteMode.OVERWRITE)\n" +
                "\t\t\t.setParallelism(1);\n\n" +
                "\t\tenv.execute(\"SPARQL Query to Flink Progran - DataStream API\");\n" +

                "\t}\n}";

        return flinkProgram;
    }
}
