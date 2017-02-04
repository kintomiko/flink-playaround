package indi.kin.flink.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * Created by kinlin on 2/4/17.
 */
public class MeetupConnectorSource extends RichSourceFunction<MeetupRsvpEvent> {

    private final MeetupStreamApiClient client;
    private volatile boolean isRunning = true;
    private final MeetupRsvpEventParser parser;

    public MeetupConnectorSource(MeetupStreamApiClient client, MeetupRsvpEventParser parser) {
        this.client = client;
        this.parser = parser;
    }

    @Override
    public void close() throws Exception {
        super.close();
        client.close();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client.connect();
    }

    @Override
    public void run(SourceContext<MeetupRsvpEvent> sourceContext) throws Exception {
        while(isRunning){
            MeetupRsvpEvent event = parser.parse(client.readOne());
            sourceContext.collect(event);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
