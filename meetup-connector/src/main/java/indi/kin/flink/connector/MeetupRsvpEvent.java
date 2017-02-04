package indi.kin.flink.connector;

/**
 * Created by kinlin on 2/4/17.
 */
public class MeetupRsvpEvent {
    private String eventName;
    private String eventId;
    private Long time;
    private String eventUrl;
    private String memberName;
    private Long memberId;

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getEventUrl() {
        return eventUrl;
    }

    public void setEventUrl(String eventUrl) {
        this.eventUrl = eventUrl;
    }

    public String getMemberName() {
        return memberName;
    }

    public void setMemberName(String memberName) {
        this.memberName = memberName;
    }

    public Long getMemberId() {
        return memberId;
    }

    public void setMemberId(Long memberId) {
        this.memberId = memberId;
    }

    @Override
    public String toString() {
        return "MeetupRsvpEvent{" +
                "eventName='" + eventName + '\'' +
                ", eventId=" + eventId +
                ", time=" + time +
                ", eventUrl='" + eventUrl + '\'' +
                ", memberName='" + memberName + '\'' +
                ", memberId=" + memberId +
                '}';
    }
}
