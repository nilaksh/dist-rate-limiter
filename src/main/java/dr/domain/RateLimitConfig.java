package dr.domain;

public interface RateLimitConfig {

	Integer getThreshold();
	Integer getBucketSize();
	Integer getRate();
	Integer getTimeSlice();
	String getKey();

}
