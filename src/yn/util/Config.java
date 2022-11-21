package yn.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

/**
 * @author	: y0ng94
 * @version	: 1.0
 */
public class Config {
	// Keep the information read from the properties in the map.
	private static HashMap<String, String> config = new HashMap<String, String>();
	
	/**
	 * Read the properties in the inputted property path and put them in the map.
	 * @param configLocation
	 * @throws IOException
	 */
	public static void setConfig(String configLocation) throws IOException {
		Properties prop = new Properties();
		
		try {
			prop.load(new FileInputStream(configLocation));
		} catch (FileNotFoundException e) {
			throw new FileNotFoundException("Configuration File not Found");
		} catch (IOException e) {
			throw new IOException(e);
		}
		
		prop.entrySet().forEach(t -> setConfig(t.getKey(), t.getValue()));
	}
	
	/**
	 * Store the inputted Object key and value in the map.
	 * @param key
	 * @param value
	 */
	public static void setConfig(Object key, Object value) {
		if (key != null) { setConfig(key.toString(), value!=null?value.toString():null); }
	}

	/**
	 * Store the inputted key and value in the map.
	 * @param key
	 * @param value
	 */
	public static void setConfig(String key, String value) {
		config.put(key, value);
	}
	
	/**
	 * Returns a value in the form of a string that is symmetrical to the key.
	 * @param key
	 * @return String
	 * @throws NullPointerException
	 */
	public static String getConfig(String key) throws NullPointerException {
		String value = config.get(key);
		if (value == null) { throw new NullPointerException("Configuration [ " + key + " ] is Null"); }
		return value;
	}
	
	/**
	 * Returns a value in the form of a integer that is symmetrical to the key.
	 * @param key
	 * @return int
	 * @throws NumberFormatException
	 */
	public static int getIntConfig(String key) throws NumberFormatException {
		return Integer.parseInt(config.get(key));
	}
	
	/**
	 * Returns a value in the form of a array that is symmetrical to the key.
	 * @param key
	 * @return String[]
	 */
	public static String[] getArrConfig(String key) {
		return config.get(key).split(",");
	}
	
	/**
	 * Returns the setting information in the form of a table.
	 * @return String
	 */
	public static String getConfigTable() {
		final int keyMaxWidth = 50;
		final int valMaxWidth = 50;
		final int keyMinWidth = 6;
		final int valMinWidth = 6;
		int keyWidth = 0;
		int valWidth = 0;

		for (Map.Entry<String, String> entry : config.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();

			keyWidth = key.length() > keyWidth ? key.length() : keyWidth;
			valWidth = value.length() > valWidth ? value.length() : valWidth;
		}

		if (keyWidth > keyMaxWidth)
			keyWidth = keyMaxWidth;
		else if (keyWidth < keyMinWidth)
			keyWidth = keyMinWidth;
			
		if (valWidth > valMaxWidth)
			valWidth = valMaxWidth;
		else if (valWidth < valMinWidth)
			valWidth = valMinWidth;

		StringBuilder strToPrint = new StringBuilder();
		StringBuilder rowSeparator = new StringBuilder();

		for (int i=0; i<2; i++) {
			String name = (i==0)?"NAME":"DATA";
			int width = (i==0)?keyWidth:valWidth;
			int diff = width - name.length();

			String toPrint;

			if ((diff % 2) == 1) {
				width++;
				diff++;
				if (i == 0)
					keyWidth = width;
				else
					valWidth = width;
			}

			int paddingSize = diff/2;
			String padding = new String(new char[paddingSize]).replace("\0", " ");

			toPrint = "| " + padding + name + padding + " ";

			strToPrint.append(toPrint);

			rowSeparator.append("+");
			rowSeparator.append(new String(new char[width + 2]).replace("\0", "-"));
		}
		
		StringBuffer buffer = new StringBuffer();
		String lineSeparator = System.getProperty("line.separator");
		lineSeparator = lineSeparator == null ? "\n" : lineSeparator;

		rowSeparator.append("+").append(lineSeparator);

		strToPrint.append("|").append(lineSeparator);
		strToPrint.insert(0, rowSeparator);
		strToPrint.append(rowSeparator);

		buffer.append(strToPrint.toString());

		String format;
		
		List<Entry<String, String>> entryList = new ArrayList<Entry<String, String>>(config.entrySet());

		Collections.sort(entryList, new Comparator<Entry<String, String>>() {
			public int compare(Entry<String, String> obj1, Entry<String, String> obj2) {
				return obj1.getKey().compareTo(obj2.getKey());
			}
		});

		for (Entry<String, String> entry : entryList) {
			String key = entry.getKey();
			String value = entry.getValue();

			if (key.length() > keyMaxWidth)
				key = key.substring(0, keyMaxWidth - 3) + "...";

			if (value.length() > valMaxWidth)
				value = value.substring(0, valMaxWidth - 3) + "...";

			format = String.format("| %%%s%ds ", "-", keyWidth);
			buffer.append(String.format(format, key));

			format = String.format("| %%%s%ds ", "-", valWidth);
			buffer.append(String.format(format, value));

			buffer.append("|\n");
			buffer.append(rowSeparator);
		}

		buffer.append("\n");

		return buffer.toString();
	}
}
