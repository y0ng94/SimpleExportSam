package yn.util;

import java.text.MessageFormat;

import org.slf4j.Logger;

/**
 * @author	: y0ng94
 * @version	: 1.0
 */
public class LogUtil {
	
	public static void error(Logger logger, String msg) {
		logger.error(msg);
	}
	
	public static void error(Logger logger, String msg, Object arg) {
		error(logger, MessageFormat.format(msg, arg));
	}
	
	public static void error(Logger logger, String msg, Object... args) {
		error(logger, MessageFormat.format(msg, args));
	}
	
	public static void warn(Logger logger, String msg) {
		logger.warn(msg);
	}
	
	public static void warn(Logger logger, String msg, Object arg) {
		warn(logger, MessageFormat.format(msg, arg));
	}
	
	public static void warn(Logger logger, String msg, Object... args) {
		warn(logger, MessageFormat.format(msg, args));
	}
	
	public static void info(Logger logger, String msg) {
		logger.info(msg);
	}
	
	public static void info(Logger logger, String msg, Object arg) {
		info(logger, MessageFormat.format(msg, arg));
	}
	
	public static void info(Logger logger, String msg, Object... args) {
		info(logger, MessageFormat.format(msg, args));
	}
	
	public static void debug(Logger logger, String msg) {
		logger.debug(msg);
	}
	
	public static void debug(Logger logger, String msg, Object arg) {
		debug(logger, MessageFormat.format(msg, arg));
	}
	
	public static void debug(Logger logger, String msg, Object... args) {
		debug(logger, MessageFormat.format(msg, args));
	}
}
