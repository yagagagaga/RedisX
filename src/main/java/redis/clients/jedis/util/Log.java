package redis.clients.jedis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Log {
	Logger log = LoggerFactory.getLogger(Thread.currentThread().getStackTrace()[1].getClassName());

	default void ifTrace(String message) {
		if (log.isTraceEnabled())
			trace(message);
	}

	default void trace(String message, Object... values) {
		log.trace(checkFormat(message, values));
	}

	default void trace(String message, Throwable error, Object... values) {
		log.trace(checkFormat(message, values), error);
	}

	default void ifDebug(String message) {
		if (log.isDebugEnabled())
			debug(message);
	}

	default void debug(String message, Object... values) {
		log.debug(checkFormat(message, values));
	}

	default void debug(String message, Throwable error, Object... values) {
		log.debug(checkFormat(message, values), error);
	}

	default void ifInfo(String message) {
		if (log.isInfoEnabled())
			info(message);
	}

	default void info(String message, Object... values) {
		log.info(checkFormat(message, values));
	}

	default void info(String message, Throwable error, Object... values) {
		log.info(checkFormat(message, values), error);
	}

	default void ifWarn(String message) {
		if (log.isWarnEnabled())
			warn(message);
	}

	default void warn(String message, Object... values) {
		log.warn(checkFormat(message, values));
	}

	default void warn(String message, Throwable error, Object... values) {
		log.warn(checkFormat(message, values), error);
	}

	default void ifError(String message) {
		if (log.isErrorEnabled())
			error(message);
	}

	default void error(String message, Object... values) {
		log.error(checkFormat(message, values));
	}

	default void error(String message, Throwable error, Object... values) {
		log.error(checkFormat(message, values), error);
	}

	default String checkFormat(String msg, Object... refs) {
		if (refs.length != 0)
			return String.format(msg, refs);
		else
			return msg;
	}

}
