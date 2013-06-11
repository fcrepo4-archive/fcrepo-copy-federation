
package org.fcrepo.sequencer.copier;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Gregory Jansen
 *
 */
public class CopyEventListener implements javax.jcr.observation.EventListener {

	private static final Logger logger = LoggerFactory
			.getLogger(CopyEventListener.class);

	private final CountDownLatch latch;

	private volatile String sequencedNodePath;

	private volatile boolean successfulSequencing = false;

	public CopyEventListener(CountDownLatch latch) {
		this.latch = latch;
	}

	@Override
	public void onEvent(javax.jcr.observation.EventIterator events) {
		logger.debug("got events: " + events.getSize());
		try {
			javax.jcr.observation.Event event = events.nextEvent();
			logger.debug("event: " + event.getPath() + " " +
					event.getIdentifier() + " " +event.getType());
			this.sequencedNodePath = event.getPath();
			this.successfulSequencing = event.getType() == org.modeshape.jcr.api.observation.Event.Sequencing.NODE_SEQUENCED;
			latch.countDown();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isSequencingSuccessful() {
	    return this.successfulSequencing;
	}

	public String getSequencedNodePath() {
		return sequencedNodePath;
	}
}
