package sparql2flinkhdt.out;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import sparql2flinkhdt.runner.functions.*;
import sparql2flinkhdt.runner.LoadTriples;
import sparql2flinkhdt.runner.functions.order.*;
import java.math.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.RuntimeExecutionMode;


import  org.apache.flink.api.java.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

public class Query {
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


/*DataStream<SolutionMapping> sm3 = sm1.leftOuterJoin(sm2)
			.where(new JoinKeySelector(new String[]{"?person"}))
			.equalTo(new JoinKeySelector(new String[]{"?person"}))
			.with(new LeftJoin());*/

//		DataStream<SolutionMapping> sm3 = sm1
//				.keyBy(new JoinKeySelector(new String[]{"?person"}))
//				.coGroup(sm2.keyBy(new JoinKeySelector(new String[]{"?person"})))
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.equalFields("predicate")
//				.with(new LeftJoin());

//		DataStream<SolutionMapping> sm3 = sm1
//				.keyBy(new JoinKeySelector(new String[]{"?person"}))
//				.coGroup(sm2.keyBy(new JoinKeySelector(new String[]{"?person"})))
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.withFields("predicate")
//				.with(new LeftJoin());

//		DataStream<SolutionMapping> sm3 = sm1
//				.keyBy(new JoinKeySelector(new String[]{"?person"}))
//				.coGroup(sm2.keyBy(new JoinKeySelector(new String[]{"?person"})))
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.select("predicate")
//				.with(new LeftJoin());


//		DataStream<SolutionMapping> sm3 = sm1
//				.keyBy(new JoinKeySelector(new String[]{"?person"}))
//				.coGroup(sm2.keyBy(new JoinKeySelector(new String[]{"?person"})))
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.project("predicate")
//				.with(new LeftJoin());

//		DataStream<SolutionMapping> sm3 = sm1
//				.keyBy(new JoinKeySelector(new String[]{"?person"}))
//				.coGroup(sm2.keyBy(new JoinKeySelector(new String[]{"?person"})))
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.fields("predicate")
//				.with(new LeftJoin());

		DataStream<SolutionMapping> sm3 = sm1
				.keyBy(new JoinKeySelector(new String[]{"?person"}))
				.coGroup(sm2.keyBy(new JoinKeySelector(new String[]{"?person"})))
				.where(new JoinKeySelector(new String[]{"?person"}))
				.equalTo(new JoinKeySelector(new String[]{"?person"}))
				.project("predicate")
				.with(new LeftJoin());

		DataStream<SolutionMapping> sm4 = sm3
				.map(new Project(new String[]{"?person", "?name", "?mbox"}));

		//************ Sink  ************
		sm4.writeAsText(params.get("output")+"Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1);

		env.execute("SPARQL Query to Flink Progran - DataStream API");
	}
}

//		DataStream<SolutionMapping> sm3 = sm1
//				.keyBy(new JoinKeySelector(new String[]{"?person"}))
//				.connect(sm2.keyBy(new JoinKeySelector(new String[]{"?person"})))
//				.process(new CoProcessFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//					List<SolutionMapping> leftList = new ArrayList<>();
//					List<SolutionMapping> rightList = new ArrayList<>();
//
//					@Override
//					public void processElement1(SolutionMapping left, Context ctx, Collector<SolutionMapping> out) throws Exception {
//						leftList.add(left);
//						for (SolutionMapping right : rightList) {
//							SolutionMapping join = left.join(right);
//							if (join != null) {
//								out.collect(join);
//							}
//						}
//					}
//
//					@Override
//					public void processElement2(SolutionMapping right, Context ctx, Collector<SolutionMapping> out) throws Exception {
//						rightList.add(right);
//						for (SolutionMapping left : leftList) {
//							SolutionMapping join = left.join(right);
//							if (join != null) {
//								out.collect(join);
//							}
//						}
//					}
//				});



//		DataStream<SolutionMapping> sm3 = sm1
//				.keyBy(new JoinKeySelector(new String[]{"?person"}))
//				.coGroup(sm2.keyBy(new JoinKeySelector(new String[]{"?person"})))
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//                .with(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//
//			@Override
//			public void coGroup(Iterable<SolutionMapping> left, Iterable<SolutionMapping> right, Collector<SolutionMapping> out) throws Exception {
//				// Implementa la lógica para procesar los dos flujos de entrada
//				// y producir un resultado combinado
//			}
//		});




//
//		DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.coGroup(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//					@Override
//					public void coGroup(Iterable<SolutionMapping> iterable, Iterable<SolutionMapping> iterable1, Collector<SolutionMapping> collector) throws Exception {
//						// Lógica de procesamiento de elementos
//					}
//				})
//				.output(new OutputSelector<SolutionMapping>() {
//					@Override
//					public Iterable<String> select(SolutionMapping sm) {
//						// Lógica para etiquetar los resultados producidos por el coGroup
//						return Collections.singleton("output");
//					}
//				});
//
//






//				DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.with(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//
//					@Override
//					public void coGroup(Iterable<SolutionMapping> left, Iterable<SolutionMapping> right, Collector<SolutionMapping> out) throws Exception {
//						// Aquí se implementa la lógica para procesar los dos flujos de entrada
//						// y producir un resultado combinado
//						List<SolutionMapping> leftList = new ArrayList<>();
//						List<SolutionMapping> rightList = new ArrayList<>();
//
//						for (SolutionMapping s : left) {
//							leftList.add(new SolutionMapping());
//						}
//						for (SolutionMapping s : right) {
//							rightList.add(new SolutionMapping());
//						}
//
//						for (SolutionMapping l : leftList) {
//							boolean matched = false;
//							for (SolutionMapping r : rightList) {
//								SolutionMapping join = l.join(r);
//								if (join != null) {
//									out.collect(join);
//									matched = true;
//								}
//							}
//							if (!matched) {
//								out.collect(l);
//							}
//						}
//					}
//				});




//		DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//                .with(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//			@Override
//			public void coGroup(Iterable<SolutionMapping> first, Iterable<SolutionMapping> second, Collector<SolutionMapping> out) throws Exception {
//				// lógica de procesamiento de los elementos
//			}
//		});





//
//		DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
//				.where(new KeySelector<SolutionMapping, String>() {
//					@Override
//					public String getKey(SolutionMapping value) throws Exception {
//						return value.get("?person").toString();
//					}
//				})
//				.equalTo(new KeySelector<SolutionMapping, String>() {
//					@Override
//					public String getKey(SolutionMapping value) throws Exception {
//						return value.get("?person").toString();
//					}
//				})
//				.with(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//					@Override
//					public void coGroup(Iterable<SolutionMapping> left, Iterable<SolutionMapping> right,
//										Collector<SolutionMapping> out) throws Exception {
//						{
//						// Aquí se implementa la lógica para procesar los dos flujos de entrada
//						// y producir un resultado combinado
//						List<SolutionMapping> leftList = new ArrayList<>();
//						List<SolutionMapping> rightList = new ArrayList<>();
//
//						for (SolutionMapping s : left) {
//							leftList.add(new SolutionMapping());
//						}
//						for (SolutionMapping s : right) {
//							rightList.add(new SolutionMapping());
//						}
//
//						for (SolutionMapping l : leftList) {
//							boolean matched = false;
//							for (SolutionMapping r : rightList) {
//								SolutionMapping join = l.join(r);
//								if (join != null) {
//									out.collect(join);
//									matched = true;
//								}
//							}
//							if (!matched) {
//								out.collect(l);
//							}
//						}
//					}
//				});










//
//		DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.coGroup(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//
//public void coGroup(Iterable<SolutionMapping> left, Iterable<SolutionMapping> right, Collector<SolutionMapping> out) throws Exception {
//						// Aquí se implementa la lógica para procesar los dos flujos de entrada
//						// y producir un resultado combinado
//						List<SolutionMapping> leftList = new ArrayList<>();
//						List<SolutionMapping> rightList = new ArrayList<>();
//
//						for (SolutionMapping s : left) {
//							leftList.add(new SolutionMapping());
//						}
//						for (SolutionMapping s : right) {
//							rightList.add(new SolutionMapping());
//						}
//
//						for (SolutionMapping l : leftList) {
//							boolean matched = false;
//							for (SolutionMapping r : rightList) {
//								SolutionMapping join = l.join(r);
//								if (join != null) {
//									out.collect(join);
//									matched = true;
//								}
//							}
//							if (!matched) {
//								out.collect(l);
//							}
//						}
//					}
//				});




//		DataStream<SolutionMapping> sm3 = sm1
//				.coGroup(sm2)
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.with(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//					@Override
//					public void coGroup(Iterable<SolutionMapping> left, Iterable<SolutionMapping> right,
//										Collector<SolutionMapping> out) throws Exception {
//						// Implementa la lógica de la operación cogroup aquí
//						List<SolutionMapping> leftList = new ArrayList<>();
//						List<SolutionMapping> rightList = new ArrayList<>();
//
//						for (SolutionMapping s : left) {
//							leftList.add(new SolutionMapping());
//						}
//						for (SolutionMapping s : right) {
//							rightList.add(new SolutionMapping());
//						}
//
//						for (SolutionMapping l : leftList) {
//							boolean matched = false;
//							for (SolutionMapping r : rightList) {
//								SolutionMapping join = l.join(r);
//								if (join != null) {
//									out.collect(join);
//									matched = true;
//								}
//							}
//							if (!matched) {
//								out.collect(l);
//							}
//						}
//					}
//				});


//		DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.with(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//
//					@Override
//					public void coGroup(Iterable<SolutionMapping> iterable, Iterable<SolutionMapping> iterable1, Collector<SolutionMapping> collector) throws Exception {
//						// Lógica de procesamiento de elementos
//					}
//				});



//		DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.apply(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//
//					@Override
//					public void coGroup(Iterable<SolutionMapping> iterable, Iterable<SolutionMapping> iterable1, Collector<SolutionMapping> collector) throws Exception {
//						// Lógica de procesamiento de elementos
//					}
//				});
//
//




	/*	DataStream<SolutionMapping> result = sm1
				.coGroup(sm2)
				.where(new JoinKeySelector(new String[]{"?person"}))
				.equalTo(new JoinKeySelector(new String[]{"?person"}))
				.with(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {

					@Override
					public void coGroup(Iterable<SolutionMapping> iterable, Iterable<SolutionMapping> iterable1, Collector<SolutionMapping> collector) throws Exception {
						// Lógica de procesamiento de elementos
					}
				})
				.apply(new MyWindowFunction());
*/

//		DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
////				.where(new JoinKeySelector(new String[]{"?person"}))
//				.where(new JoinKeySelector("?person"))
////				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector("?person"))
//				.with(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//					@Override
//					public void coGroup(Iterable<SolutionMapping> iterable, Iterable<SolutionMapping> iterable1, Collector<SolutionMapping> collector) throws Exception {
//						// Lógica de procesamiento de elementos
//					}
//				});
//

//		DataStream<SolutionMapping> sm3 = sm1
//				.keyBy(new KeySelector<SolutionMapping, String>() {
//					@Override
//					public String getKey(SolutionMapping sm) throws Exception {
//						return sm.get("?person").toString();
//					}
//				})
//				.cogroup(sm2.keyBy(new KeySelector<SolutionMapping, String>() {
//					@Override
//					public String getKey(SolutionMapping sm) throws Exception {
//						return sm.get("?person").toString();
//					}
//				}))
//				.where(new LeftJoinFirstKeySelector())
//				.equalTo(new LeftJoinSecondKeySelector())
//				.with(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//					@Override
//					public void coGroup(Iterable<SolutionMapping> first, Iterable<SolutionMapping> second, Collector<SolutionMapping> out) throws Exception {
//						SolutionMapping sm = new SolutionMapping();
//						boolean matched = false;
//						for (SolutionMapping left : first) {
//							matched = true;
//							for (SolutionMapping right : second) {
//								sm.putAll(left);
//								sm.putAll(right);
//								out.collect(sm);
//							}
//						}
//						if (!matched) {
//							for (SolutionMapping right : second) {
//								sm.putAll(right);
//								out.collect(sm);
//							}
//						}
//					}
//				});




//		DataStream<SolutionMapping> sm3 = sm1
//				.keyBy(new JoinKeySelector(new String[]{"?person"}))
//				.coGroup(sm2.keyBy(new JoinKeySelector(new String[]{"?person"})))
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.with(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//					@Override
//					public void coGroup(Iterable<SolutionMapping> first, Iterable<SolutionMapping> second, Collector<SolutionMapping> out) {
//						for (SolutionMapping sm1 : first) {
//							boolean matchFound = false;
//							for (SolutionMapping sm2 : second) {
//								if (sm1.get("?person").equals(sm2.get("?person"))) {
//									SolutionMapping sm = new SolutionMapping();
//									sm.addAll(sm1);
//									sm.addAll(sm2);
//									out.collect(sm);
//									matchFound = true;
//								}
//							}
//							if (!matchFound) {
//								out.collect(sm1);
//							}
//						}
//					}
//				});


//
//		DataStream<SolutionMapping> sm3 = sm1.coGroup(sm2)
//				.where(new JoinKeySelector(new String[]{"?person"}))
//				.equalTo(new JoinKeySelector(new String[]{"?person"}))
//				.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
//				.apply(new CoGroupFunction<SolutionMapping, SolutionMapping, SolutionMapping>() {
//					@Override
//					public void coGroup(Iterable<SolutionMapping> first, Iterable<SolutionMapping> second,
//										Collector<SolutionMapping> out) throws Exception {
//						List<SolutionMapping> list1 = new ArrayList<>();
//						List<SolutionMapping> list2 = new ArrayList<>();
//						for (SolutionMapping sm : first) {
//							list1.add(sm);
//						}
//						for (SolutionMapping sm : second) {
//							list2.add(sm);
//						}
//						for (SolutionMapping sm1 : list1) {
//							boolean found = false;
//							for (SolutionMapping sm2 : list2) {
//								if (sm1.get("?person").equals(sm2.get("?person"))) {
//									found = true;
//									break;
//								}
//							}
//							if (!found) {
//								out.collect(sm1);
//							}
//						}
//					}
//				});
