package redis.clients.jedis.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class PropertiesPlus extends Properties {
	public static PropertiesPlus load(String path) {
		InputStream in = null;
		try {
			in = PropertiesPlus.class.getClassLoader().getResourceAsStream(path);
			if (null == in) {
				if (new File(path).exists()) {
					in = new FileInputStream(path);
				} else {
					throw new FileNotFoundException("Could not load " + path);
				}
			}
			final PropertiesPlus props = new PropertiesPlus();
			props.load(in);
			return props;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException ignored) {
				}
			}
		}
	}

	public String getString(String key, String defVal) {
		return super.getProperty(key, defVal);
	}

	public Set<String> getSet(String key, String delimiter) {
		final String value = getProperty(key);
		if (value == null) {
			return Collections.emptySet();
		} else {
			final String[] split = value.split(delimiter);
			return Arrays.stream(split).collect(Collectors.toSet());
		}
	}

	public List<String> getList(String key, String delimiter) {
		final String value = getProperty(key);
		if (value == null) {
			return Collections.emptyList();
		} else {
			final String[] split = value.split(delimiter);
			return Arrays.asList(split);
		}
	}

	public Duration getDuration(String key, Duration defVal) {
		final String value = getProperty(key);
		if (value == null) {
			return defVal;
		} else {
			try {
				return Duration.parse(value);
			} catch (DateTimeParseException e) {
				final int number = Integer.parseInt(value.substring(0, value.length() - 1).trim());
				final char unit = value.charAt(value.length() - 1);
				switch (Character.toLowerCase(unit)) {
				case 's':
					return Duration.ofSeconds(number);
				case 'm':
					return Duration.ofMinutes(number);
				case 'h':
					return Duration.ofHours(number);
				case 'd':
					return Duration.ofDays(number);
				default:
					throw new IllegalArgumentException("Could Not Parse：" + value);
				}
			}
		}
	}

	public Integer getInteger(String key, Integer defVal) {
		final String value = getProperty(key);
		if (value == null) {
			return defVal;
		} else {
			return Integer.parseInt(value);
		}
	}

	public Boolean getBoolean(String key, Boolean defVal) {
		final String value = getProperty(key);
		if (value == null) {
			return defVal;
		} else {
			return Boolean.parseBoolean(value);
		}
	}

	public Long getLong(String key, Long defVal) {
		final String value = getProperty(key);
		if (value == null) {
			return defVal;
		} else {
			return Long.parseLong(value);
		}
	}
}
