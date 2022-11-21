package yn.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author	: y0ng94
 * @version	: 1.0
 */
public class TablePrinterUtil {
	// Maximum column width default value
	private static final int DEFAULT_MAX_COL_WIDTH	= 150;

	// Column DTO
	private static class Column {
		private int number = 0;			
		private String label = "";
		private int width = 0;
		private List<String> values = new ArrayList<>();
        private String justifyFlag = "";

		public int getNumber() {
			return this.number;
		}

		public void setNumber(int number) {
			this.number = number;
		}

		public String getLabel() {
			return this.label;
		}

		public void setLabel(String label) {
			this.label = label;
		}

		public int getWidth() {
			return this.width;
		}

		public void setWidth(int width) {
			this.width = width;
		}

		public String getValue(int i) {
			return this.values.get(i);
		}

		public void addValue(String value) {
            this.values.add(value);
		}

        public String getJustifyFlag() {
            return justifyFlag;
        }

        public void justifyLeft() {
            this.justifyFlag = "-";
        }

        public void justifyRight() {
            this.justifyFlag = "";
        }
	}

	/**
	 * Returns the information received in a table format
	 * @param colArr
	 * @param val
	 * @param maxColWidth
	 * @return String
	 */
	public static String printTable(String[] colArr, Map<String, Object> val, int maxColWidth) {
		List<Map<String, Object>> valList = new ArrayList<>();
		valList.add(val);
		return printTable(colArr, valList, maxColWidth);
	}

	/**
	 * Returns the information received in a table format
	 * @param colArr
	 * @param valList
	 * @param maxColWidth
	 * @return String
	 */
	public static String printTable(String[] colArr, List<Map<String, Object>> valList, int maxColWidth) {
		List<Column> columnList = new ArrayList<>();
		maxColWidth = (maxColWidth < 1) ? DEFAULT_MAX_COL_WIDTH : maxColWidth;

		for (int i=0; i<colArr.length; i++) {
			Column column = new Column();
			column.setNumber(i+1);
			column.setLabel(colArr[i]);
			column.setWidth(column.getLabel().length());
			columnList.add(column);
		}

		for (Map<String, Object> val : valList) {
			for (int i=0; i<colArr.length; i++) {
				int num = i+1;
				Column column = columnList.stream().filter(x -> (x.getNumber() == (num))).findFirst().orElseThrow(IllegalArgumentException::new);
				Object value = val.get(colArr[i]);
				String convertValue = "";

				if (value instanceof Integer) {
					convertValue = String.format("%d", value);
					column.justifyRight();
				} else if (value instanceof Float || value instanceof Double) {
					convertValue = String.valueOf(value);
					column.justifyRight();
				} else if (value instanceof String) {
					convertValue = (String) value;
					column.justifyLeft();
				} else if (value instanceof Integer[]) {
					Integer[] intArr = (Integer[]) value;
					String[] strArr = new String[intArr.length];
					for (int j=0; j<intArr.length; j++)
						strArr[j] = String.valueOf(intArr[j]);
					convertValue = Arrays.toString(strArr);
					column.justifyLeft();
				} else if (value instanceof String[]) {
					convertValue = Arrays.toString((String[]) value);
					column.justifyLeft();
				}
				if (convertValue.length() > maxColWidth)
					convertValue = convertValue.substring(0, maxColWidth - 3) + "...";
				
				val.put(column.getLabel(), convertValue);
				
				column.setWidth(convertValue.length() > column.getWidth() ? convertValue.length() : column.getWidth());
				column.addValue(convertValue);
			}
		}

		StringBuilder strToPrint = new StringBuilder();
		StringBuilder rowSeparator = new StringBuilder();

		for (Column column : columnList) {
			String name = column.getLabel();
			int width = column.getWidth();
			int diff = width - name.length();
			String toPrint;

			if ((diff % 2) == 1) {
				width++;
				diff++;
				column.setWidth(width);
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

		for (int i=0; i<valList.size(); i++) {
			for (Column column : columnList) {
				format = String.format("| %%%s%ds ", column.getJustifyFlag(), column.getWidth());
				buffer.append(String.format(format, column.getValue(i)));
			}

			buffer.append("|\n");
			buffer.append(rowSeparator);
		}

		buffer.append("\n");

		return buffer.toString();
	}
}
