package indi.kin.flink.connector;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by kinlin on 2/4/17.
 */
public class MeetupStreamApiClient implements Serializable{
    private static final String url = "http://stream.meetup.com/2/rsvps";

    private BufferedReader bufferedReader;

    public String readOne() throws IOException {
        return bufferedReader.readLine();
    }

    public void connect() throws IOException {
        HttpURLConnection connection;
        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
        } catch (IOException e) {
            throw new RuntimeException();
        }
        connection.setRequestProperty("User-Agent", "Java");
        InputStream inputStream = connection.getInputStream();
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
    }

    public void close() throws IOException {
        bufferedReader.close();
    }
}
