package sparql2flinkhdt.out;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Job {

    public static class Person implements Serializable {
        public int id;
        public String name;

        public Person() {
            this(-1, "");
        }

        public Person(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static class Score implements Serializable {
        public Person person;
        public int subject;
        public int score;

        public Score() {
            this(null, -1, -1);
        }

        public Score(Person person, int subject, int score) {
            this.person = person;
            this.subject = subject;
            this.score = score;
        }
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final List<Person> people = new ArrayList<Person>() {{
            add(new Person(1, "Chiwan"));
            add(new Person(2, "Jinsoo"));
        }};

        final List<Score> scores = new ArrayList<Score>() {{
            add(new Score(people.get(0), 1, 70));
            add(new Score(people.get(0), 2, 65));
            add(new Score(people.get(1), 1, 65));
            add(new Score(people.get(1), 2, 70));
        }};

        DataSet<Person> peopleSet = env.fromCollection(people);
        DataSet<Score> scoreSet = env.fromCollection(scores);

        List<Tuple2<Person, Integer>> joinResult = peopleSet
                .join(scoreSet)
                .where("*")
                .equalTo("person")
                .with(new JoinFunction<Person, Score, Tuple2<Person, Integer>>() {
                    @Override
                    public Tuple2<Person, Integer> join(Person first, Score second) throws Exception {
                        return new Tuple2<>(first, second.score);
                    }
                }).collect();

        List<Tuple2<Person, Integer>> cogroupResult = peopleSet
                .coGroup(scoreSet)
                .where("*")
                .equalTo("person")
                .with(new CoGroupFunction<Person, Score, Tuple2<Person, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Person> people, Iterable<Score> scores, Collector<Tuple2<Person, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Score score : scores) {
                            sum += score.score;
                        }

                        out.collect(new Tuple2<>(people.iterator().next(), sum));
                    }
                }).collect();
    }
}

