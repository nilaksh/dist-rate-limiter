package dr.exception;

public class RateExceededException extends RuntimeException{

	public RateExceededException(String key) {
		super(String.format("too many requests per second for %s", key));
	}
	
}
