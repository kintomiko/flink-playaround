package indi.kin.flink.connector;

import org.json.JSONObject;

import java.io.Serializable;

/**
 * Created by kinlin on 2/4/17.
 */
public class MeetupRsvpEventParser implements Serializable {
    public MeetupRsvpEvent parse(String jsonString) {
        JSONObject json = new JSONObject(jsonString);

        MeetupRsvpEvent event = new MeetupRsvpEvent();
        event.setEventId(json.getJSONObject("event").getString("event_id"));
        event.setEventName(json.getJSONObject("event").getString("event_name"));
        event.setEventUrl(json.getJSONObject("event").getString("event_url"));
        event.setMemberId(json.getJSONObject("member").getLong("member_id"));
        event.setMemberName(json.getJSONObject("member").getString("member_name"));
        event.setTime(json.getLong("mtime"));
        return event;
    }
}
