package org.fcrepo.sequencer.copier;

import static org.modeshape.jcr.api.observation.Event.Sequencing.NODE_SEQUENCED;
import static org.slf4j.LoggerFactory.getLogger;
import gov.loc.repository.bagit.Bag;
import gov.loc.repository.bagit.BagFactory;
import gov.loc.repository.bagit.PreBag;
import gov.loc.repository.bagit.transformer.impl.DefaultCompleter;
import gov.loc.repository.bagit.writer.impl.FileSystemWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.observation.Event;
import javax.jcr.observation.ObservationManager;

import org.fcrepo.sequencer.copier.LoggingEventListener.EventLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * @author Gregory Jansen
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/spring-test/master.xml")
public class NodeCopySequencerIT {

    private static Logger logger = getLogger(NodeCopySequencerIT.class);

    @Inject
    Repository repo;

    Session listenerSession;

    @Before
    public void prepare() throws RepositoryException {
        final Session session = repo.login("default");
        session.getRootNode().addNode("objects", "nt:folder");
        session.getRootNode().addNode("federated", "nt:folder");
        session.save();
        this.listenerSession = repo.login("default");
    }

    @After
    public void after() {
        listenerSession.logout();
    }

	@Test
	public void testBagSequencerCopied() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        EventLogger elog = getEventLogger(latch, Event.NODE_ADDED | NODE_SEQUENCED);

        Session session = repo.login("default");

        // create a random bag and move it into the federated directory
        final File baseDir = new File("./target/test-classes");
        final File srcDir = new File(baseDir, "tmp-objects");
        final File dstDir = new File(baseDir, "test-objects");
        final long fileSize = 1024L;
        makeRandomBags(srcDir, 1, 1, fileSize);
        final File srcBag = new File(srcDir, "randomBag0");
        final File dstBag = new File(dstDir, "randomBagSequencerCopy");
        srcBag.renameTo(dstBag);

        Node n = session.getNode("/federated/randomBagSequencerCopy");
        logger.info("got node: "+n.toString());
        session.logout();

        // Now, block until the latch is decremented (by the listener) or when our max wait time is exceeded
        latch.await(15, TimeUnit.SECONDS);
        // verify
	}


    static void makeRandomBags(final File baseDir, final int bagCount,
            final int fileCount, final long fileSize) throws IOException {
        final gov.loc.repository.bagit.BagFactory factory = new BagFactory();
        final DefaultCompleter completer = new DefaultCompleter(factory);
        final FileSystemWriter writer = new FileSystemWriter(factory);
        for (int i = 0; i < bagCount; i++) {
            logger.debug("Creating random bag: " + i);
            final File bagDir = new File(baseDir, "randomBag" + i);
            final File dataDir = new File(bagDir, "data");
            if (!dataDir.exists()) {
                dataDir.mkdirs();
            }
            for (int j = 0; j < fileCount; j++) {
                final File dataFile = new File(dataDir, "randomFile" + j);
                final BufferedWriter buf =
                        new BufferedWriter(new FileWriter(dataFile));
                for (long k = 0L; k < fileSize; k++) {
                    buf.write(String.valueOf((int) (Math.random() * 10)));
                }
                buf.close();
            }
            final PreBag pre = factory.createPreBag(bagDir);
            final Bag bag =
                    pre.makeBagInPlace(BagFactory.LATEST, true, completer);
            bag.write(writer, bagDir);
        }
    }

    /**
	 * @param addlatch
	 * @return
	 */
	private EventLogger getEventLogger(CountDownLatch latch, int eventTypes) throws RepositoryException {
        ObservationManager observationManager = this.listenerSession.getWorkspace().getObservationManager();
        boolean isDeep = true; // if outputPath is ancestor of the sequencer output, false if identical
        String[] uuids = null; // Don't care about UUIDs of nodes for sequencing events
        String[] nodeTypes = null; // Don't care about node types of output nodes for sequencing events
        boolean noLocal = false; // We do want events for sequencing happen locally (as well as remotely)
        EventLogger result = Mockito.spy(new LoggingEventListener.EventLogger());
        LoggingEventListener addlistener = new LoggingEventListener(latch, result);
        String outputPath = "/";
        observationManager.addEventListener(addlistener,eventTypes,outputPath,isDeep,
                                            uuids, nodeTypes, noLocal);
		return result;
	}
}
