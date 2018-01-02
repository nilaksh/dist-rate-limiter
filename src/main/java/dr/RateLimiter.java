package dr;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dr.domain.RateLimitConfig;
import dr.exception.RateExceededException;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * RateLimiter will throttle service calls based on per service configuration.
 * 
 * @author Nilaksh Bajpai
 */
public class RateLimiter {

	private static final Logger LOGGER = LoggerFactory.getLogger(RateLimiter.class);
	private static final String LUA_SCRIPT = "local current = redis.call(\"incrBy\",KEYS[1], tonumber(KEYS[2])) "
			+ "if tonumber(current) == tonumber(KEYS[2]) then redis.call(\"expire\",KEYS[1], tonumber(KEYS[3])) end";
	private static final ExpiringMap<String, CountDownLatch> CACHE = ExpiringMap.builder().variableExpiration().build();

	private JedisPool jedisPool;

	public RateLimiter(String host, int port) {
		try {
			if (jedisPool == null) {
				JedisPoolConfig config = new JedisPoolConfig();
				config.setMaxTotal(1000);
				config.setMaxIdle(10);
				config.setMinIdle(1);
				config.setMaxWaitMillis(30000);
				
				jedisPool = new JedisPool(config, host, port);
			}
		} catch (JedisConnectionException e) {
			LOGGER.error("Could not establish Redis connection. Is the Redis running?");
			throw e;
		}
	}

	public void checkLimit(RateLimitConfig config) {
		if (isRequiredInfoMissing(config)) {
			throw new IllegalStateException("Passed config must have bucket size, rate, time slice or key");
		}
		Jedis jedis = null;
		try {
			String key = config.getKey();
			CountDownLatch countDown = CACHE.get(key);
			if (countDown == null || countDown.getCount() <= 1) {
				countDown = new CountDownLatch(config.getBucketSize());
				CACHE.put(key, countDown, ExpirationPolicy.CREATED, config.getTimeSlice(), TimeUnit.SECONDS);
				jedis = jedisPool.getResource();
				String current = jedis.get(key);
				LOGGER.debug("Current rate {}", current);
				if (current != null && Integer.parseInt(current) > config.getRate()) {
					CACHE.remove(key);
					throw new RateExceededException(key);
				} else {
					Object response = jedis.eval(LUA_SCRIPT, 3, key, String.valueOf(config.getBucketSize()),
							String.valueOf(config.getTimeSlice()));
					LOGGER.debug("response {}", response);
				}

			} else {
				countDown.countDown();
			}
		} catch (Exception e) {
			if(e instanceof RateExceededException) {
				RateExceededException ree = (RateExceededException) e;
				throw ree;
			} else {
				LOGGER.error("Exception occured with rate-limiter:",e);
			}
		}
		finally {
			if (jedis != null)
				jedis.close();
		}

	}

	private boolean isRequiredInfoMissing(RateLimitConfig config) {
		return config == null || config.getBucketSize() == null || config.getKey() == null || config.getKey().equals("")
				|| config.getRate() == null || config.getTimeSlice() == null;
	}

}
