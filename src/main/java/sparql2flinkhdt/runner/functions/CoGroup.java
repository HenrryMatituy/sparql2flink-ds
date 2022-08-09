package sparql2flinkhdt.runner.functions;

import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.jena.graph.Node;

import java.util.Map;

public class CoGroup implements CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping> {
    @Override
    public void coGroup(Iterable<SolutionMapping> left, Iterable<SolutionMapping> right, Collector<SolutionMapping> out) throws Exception {
  out.collect(left.iterator().next());


//        out.collect(right.CoGroupp(left));
//        out.collect(new SolutionMapping());

          }


}

