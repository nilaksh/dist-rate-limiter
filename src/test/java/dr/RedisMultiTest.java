package dr;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 * RateLimiter will throttle service calls based on per service configuration.
 * 
 * @author Nilaksh Bajpai
 */
public class RedisMultiTest {
	public static void main(String[] args) {
		Jedis jedis = new Jedis();
		for (int j = 0; j < 100; j++) {
			long startTime = System.currentTimeMillis();
			for (int i = 0; i < 10000; i++) {
				atomicIncr(jedis);
			}
			System.out.println("Time taken by atomic:" + (System.currentTimeMillis() - startTime));
			startTime = System.currentTimeMillis();
			for (int i = 0; i < 10000; i++) {
				incr(jedis);
			}
			System.out.println("Time taken by incr:" + (System.currentTimeMillis() - startTime));
		}
	}

	private static void atomicIncr(Jedis jedis) {
		String key = "someKey";
		Transaction transaction = jedis.multi();
		Response<Long> response = transaction.incr(key);
		transaction.expire(key, 1);
		transaction.exec();
	}

	private static void incr(Jedis jedis) {
		String key = "someOtherKey";
		jedis.incr(key);
		jedis.expire(key, 1);
	}
}
