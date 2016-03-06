package edu.mum.cs522.logs;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessLog implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = Logger.getLogger("Access");

	private String ipAddress;
	private String clientIdentd;
	private String userID;
	private String dateTimeString;
	private String method;
	private String endpoint;
	private String protocol;
	private int responseCode;
	private long contentSize;

	private AccessLog(String ipAddress, String clientIdentd, String userID,
			String dateTime, String method, String endpoint, String protocol,
			String responseCode, String contentSize) {
		this.ipAddress = ipAddress;
		this.clientIdentd = clientIdentd;
		this.userID = userID;
		this.dateTimeString = dateTime; 
		this.method = method;
		this.endpoint = endpoint;
		this.protocol = protocol;
		this.responseCode = Integer.parseInt(responseCode);
		this.contentSize = Long.parseLong(contentSize);
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public String getClientIdentd() {
		return clientIdentd;
	}

	public String getUserID() {
		return userID;
	}

	public String getDateTimeString() {
		return dateTimeString;
	}

	public String getMethod() {
		return method;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public String getProtocol() {
		return protocol;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public long getContentSize() {
		return contentSize;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public void setClientIdentd(String clientIdentd) {
		this.clientIdentd = clientIdentd;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public void setDateTimeString(String dateTimeString) {
		this.dateTimeString = dateTimeString;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	public void setContentSize(long contentSize) {
		this.contentSize = contentSize;
	}

	private static final String LOG_ENTRY_PATTERN =
	// 1:IP 2:client 3:user 4:date time 5:method 6:req 7:proto 8:respcode 9:size
	"^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
	private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

	public static AccessLog parseFromLogLine(String logline) {
		Matcher m = PATTERN.matcher(logline);
		if (!m.find()) {
			logger.log(Level.ALL, "Cannot parse logline" + logline);
			logger.info("Error line "+ logline);
			throw new RuntimeException("Error parsing logline");
		}

		return new AccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
				m.group(5), m.group(6), m.group(7), m.group(8), m.group(9));
	}
}
