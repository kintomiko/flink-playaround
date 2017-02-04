package indi.kin.flink.connector;

import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

/**
 * Created by kinlin on 2/4/17.
 */
public class MeetupConnectorSourceTest {

    private static final int ASSERT_COUNT = 3;

    private static final String RSVP_JSON = "/MeetupRsvp.json";

    @Test
    public void testSourceParser() throws IOException {
        MeetupStreamApiClient client = mock(MeetupStreamApiClient.class);

        //given
        InputStream is = getClass().getResourceAsStream(RSVP_JSON);
        String jsonString = IOUtils.toString(is);
        given(client.readOne()).willReturn(jsonString);

        //when
        MeetupRsvpEventParser meetupEventParser = new MeetupRsvpEventParser();
        MeetupRsvpEvent event = meetupEventParser.parse(jsonString);

        //verify
        assertEquals(event.getEventName(), "Non-Gaming VR Meet-up #8: VR + Travel ");
        assertTrue(event.getEventId().equals("237383400"));
        assertTrue(event.getTime().equals(1486147964000L));
        assertEquals(event.getEventUrl(), "https://www.meetup.com/Non-Gaming-VR/events/237383400/");
        assertEquals(event.getMemberName(), "Matt");
        assertTrue(event.getMemberId().equals(217363257L));
    }

    @Test
    public void testClient() throws Exception {
        MeetupStreamApiClient client = new MeetupStreamApiClient();
        client.connect();
        for (int i = 0; i < ASSERT_COUNT; i++) {
            String input = client.readOne();
            System.out.println(input);
            assertNotNull(input);
        }
    }

    @Test
    public void testSource() throws Exception {
        //given
        MeetupStreamApiClient client = mock(MeetupStreamApiClient.class);
        MeetupRsvpEventParser parser = mock(MeetupRsvpEventParser.class);
        MeetupConnectorSource source = new MeetupConnectorSource(client, parser);

        //when
        source.open(new Configuration());
        //verify
        verify(client, times(1)).connect();

        //when
        source.close();
        //verify
        verify(client, times(1)).close();

    }

    @Test
    public void testCanelCanStop() throws Exception {
        //given
        final SourceFunction.SourceContext<MeetupRsvpEvent> context = mock(SourceFunction.SourceContext.class);

        MeetupStreamApiClient client = mock(MeetupStreamApiClient.class);
        MeetupRsvpEventParser parser = mock(MeetupRsvpEventParser.class);
        given(client.readOne()).willReturn(String.valueOf(Math.random()));

        final MeetupConnectorSource source = new MeetupConnectorSource(client, parser);

        //when
        ExecutorService executor = Executors.newFixedThreadPool(4);
        Runnable cancelTask = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000l);
                    source.cancel();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        Runnable runTask = new Runnable() {
            @Override
            public void run() {
                try {
                    source.run(context);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        executor.execute(runTask);
        executor.execute(cancelTask);

        executor.shutdown();
        Thread.sleep(1000l);
        assertFalse(executor.isTerminated());
        Thread.sleep(4000l);
        assertTrue(executor.isTerminated());

    }
}
