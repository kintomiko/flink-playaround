package wikiedits;

import indi.kin.flink.connector.MeetupConnectorSource;
import indi.kin.flink.connector.MeetupRsvpEvent;
import indi.kin.flink.connector.MeetupRsvpEventParser;
import indi.kin.flink.connector.MeetupStreamApiClient;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Created by kinlin on 2/4/17.
 */
public class MeetupAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<MeetupRsvpEvent> edits = see.addSource(new MeetupConnectorSource(new MeetupStreamApiClient(), new MeetupRsvpEventParser()));

        KeyedStream<MeetupRsvpEvent, String> keyedEdits = edits
                .keyBy(new KeySelector<MeetupRsvpEvent, String>() {
                    @Override
                    public String getKey(MeetupRsvpEvent event) {
                        return event.getEventId();
                    }
                });

        DataStream<Tuple2<String, String>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", ""), new FoldFunction<MeetupRsvpEvent, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> fold(Tuple2<String, String> acc, MeetupRsvpEvent event) {
                        acc.f0 = event.getEventId();
                        acc.f1 += new StringBuilder().append(event.getMemberId())
                            .append(":")
                            .append(event.getMemberName())
                            .append(",");
                        return acc;
                    }
                });

        result.map(new MapFunction<Tuple2<String, String>, String>() {
            @Override
            public String map(Tuple2<String, String> tuple) {
                return tuple.toString();
            }
        })
                .addSink(new FlinkKafkaProducer08<>("ec2-13-55-235-119.ap-southeast-2.compute.amazonaws.com:9092", "meetup-rsvp", new SimpleStringSchema()));

        see.execute();
    }
}
