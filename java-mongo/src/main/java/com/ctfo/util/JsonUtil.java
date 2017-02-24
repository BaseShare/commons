package com.ctfo.util;


public class JsonUtil {

	public static final String EMPTY_JSON = "[]";

	public static final String EMPTY_JSON2 = "{}";

	public static final String EMPTY_JSON_GRID = "{\"Rows\":[],"
			+ "\"Total\":\"0\"}";

	public static final String UNDO_F = "serialVersionUID";

	//private char[] chars = {'^','$','*','+','?','|','\n','\r','"','\\','/','\b','\d','\f','\t','（','）','[',']','{','}',','};

	/**
	 * 处理json特殊字符
	 * 
	 * @param results
	 * @return
	 */
	public static String jsonCharFormat(String results) {
		StringBuilder sb = new StringBuilder();
		char[] chars = ((String) results).toCharArray();
		for (char c : chars) {
			if (c == '\n') {
				sb.append("\\n");
			} else if (c == '\r') {
				sb.append("\\r");
			} else if (c == '"') {
				sb.append("\\\"");
			} else if (c == '\\') {
				sb.append("\\\\");
			} else if (c == '/') {
				sb.append("\\/");
			} else if (c == '\b') {
				sb.append("\\b");
			} else if (c == '\f') {
				sb.append("\\f");
			} else if (c == '\t') {
				sb.append("\\t");
			} else if (c == '（') {
				sb.append("\\（");
			} else if (c == '）') {
				sb.append("\\）");
			} else if (c == '(') {
				sb.append("\\(");
			} else if (c == ')') {
				sb.append("\\)");
			} else if (Character.isISOControl(c)) {
				char[] hex = "0123456789ABCDEF".toCharArray();
				sb.append("\\u");
				int n = c;
				for (int i = 0; i < 4; ++i) {
					int digit = (n & 0xf000) >> 12;
					sb.append(hex[digit]);
					n <<= 4;
				}
			} else {
				sb.append(c);
			}
		}
		return sb.toString();
	}

}
