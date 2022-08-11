package sparql2flinkhdt.runner.functions;

import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.CoGroupFunction;

public class CoGroup implements CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping> {
    @Override
    public void coGroup(Iterable<SolutionMapping> left, Iterable<SolutionMapping> right, Collector<SolutionMapping> out) throws Exception {
  out.collect(left.iterator().next());

//        SolutionMapping output = left.iterator().next();
        System.out.println(System.currentTimeMillis());
//        for (SolutionMapping l: left) {
//            System.out.println(l);
//            out.collect(output);
//        }

        System.out.println("prueba");

         }






}





