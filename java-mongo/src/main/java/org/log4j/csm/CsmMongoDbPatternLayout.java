package org.log4j.csm;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.helpers.PatternParser;
import org.apache.log4j.spi.LoggingEvent;
/**
 *  log4j init pattern class
 * @author malongqing
 * @version 1.0
 */
public class CsmMongoDbPatternLayout extends PatternLayout {
	
	private PatternConverter csmHead;

	@Override
	public PatternParser createPatternParser(String pattern){
		PatternParser parser;
		if (pattern == null){
			//parser = new PatternParser("%m%n");
			parser = new CsmHostInfoPatternParser("%m%n");
			csmHead = parser.parse();
		}else {
			//parser = new PatternParser(pattern);			
			parser = new CsmHostInfoPatternParser(pattern);	
			csmHead = parser.parse();
		}
		return parser;
	}

	@Override
	public String format(LoggingEvent event) {
		// Reset working stringbuffer
		StringBuffer sbuf = new StringBuffer(BUF_SIZE);
		if(sbuf.capacity() > MAX_CAPACITY) {
			sbuf = new StringBuffer(BUF_SIZE);
		} else {
			sbuf.setLength(0);
		}
		
		PatternConverter c = csmHead;

		while(c != null) {
			c.format(sbuf, event);
			c = c.next;
		}
		return sbuf.toString();
	}
	
	
}
