
package org.fcrepo.sequencer.copier;

import java.util.concurrent.CountDownLatch;

import javax.jcr.observation.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This event listener is just for testing that events are working.
 * @author Gregory Jansen
 *
 */
public class LoggingEventListener implements javax.jcr.observation.EventListener {

	private static final Logger logger = LoggerFactory
			.getLogger(LoggingEventListener.class);

	private final CountDownLatch latch;

	private final EventLogger log;

	public LoggingEventListener(CountDownLatch latch, EventLogger log) {
		this.latch = latch;
		this.log = log;
	}

	@Override
	public void onEvent(javax.jcr.observation.EventIterator events) {
		logger.debug("logging events: " + events.getSize());
		try {
			while(events.hasNext()) {
				Event event = events.nextEvent();
				this.log.log(event.getType(), event.getPath());
				latch.countDown();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static class EventLogger {
		public void log(int type, String path) {
			logger.debug("event: " + type + " " +path);
		}
	}
}
