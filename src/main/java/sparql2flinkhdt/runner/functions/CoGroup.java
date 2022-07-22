package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.CoGroupFunction;
import sparql2flinkhdt.runner.functions.SolutionMapping;

//SolutionMapping - Flat Join Function
public class CoGroup implements CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping> {

    @Override
     public void coGroup(Iterable<SolutionMapping> left, Iterable<SolutionMapping> right, Collector<SolutionMapping> out) throws Exception {
        out.collect(left.iterator().next());

        //    out.collect(left.coGroup(right));

//        out.collect(new SolutionMapping());

        //        out.collect(new SolutionMapping().leftJoin(right));
        //     out.collect(coGroupF.(right));

//        System.out.println(collector.toString().toString());

    }


}