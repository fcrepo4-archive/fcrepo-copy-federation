
package org.fcrepo.sequencer.copier;

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

import javax.inject.Inject;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * This tests copying projected BagIt nodes into internal nodes via Workspace.copy()
 * It also converts ExternalBinary properties into Binary properties, completely
 * migrating the projected data into Modeshape.
 * @author Gregory Jansen
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/spring-test/master.xml")
public class FederationNodeCopyIT {

	private static Logger logger = getLogger(FederationNodeCopyIT.class);

	@Inject
	Repository repo;

	@Before
	public void prepare() throws RepositoryException {
		final Session session = repo.login("default");
		session.getRootNode().addNode("objects", "nt:folder");
		session.getRootNode().addNode("federated", "nt:folder");
		session.save();
	}

	@Test
	public void testCopy() throws Exception {
		String internalPath = "/objects";
		String federatedPath = "/federated";
		String bagName = "randomBagManualCopy";
		// create a random bag and move it into the federated directory
		final File baseDir = new File("./target/test-classes");
		final File srcDir = new File(baseDir, "tmp-objects");
		final File dstDir = new File(baseDir, "test-objects");
		final long fileSize = 1024L;
		makeRandomBags(srcDir, 1, 1, fileSize);
		final File srcBag = new File(srcDir, "randomBag0");
		final File dstBag = new File(dstDir, bagName);
		srcBag.renameTo(dstBag);

		String federatedBagPath = federatedPath + "/" + bagName;
		String internalBagPath = internalPath + "/" + bagName;

		final Session session = repo.login("default");
		Node n = session.getNode(federatedBagPath);
		logger.info("got " + federatedBagPath + " node: " + n.toString());

		session.getWorkspace().copy(federatedBagPath, internalBagPath);
		Node in = session.getNode(internalBagPath);
		logger.info("Copying external binaries to internal storage.. (" +
				internalBagPath + ")");
		internalizeBinaries(in, session);
		session.save();

		FileUtils.deleteDirectory(dstBag);
		logger.debug("deleted bag directory: " + dstBag.getAbsolutePath());

		in = session.getNode(internalBagPath);
		logger.info("Testing access to copied binary.. (" + internalBagPath +
				" node: " + in.toString() + ")");
		for (NodeIterator iter = in.getNodes(); iter.hasNext();) {
			Node f = (Node) iter.next();
			if (f.isNodeType("nt:file")) {
				Node content = f.getNode("jcr:content");
				Binary bin = content.getProperty("jcr:data").getBinary();
				logger.debug("binary size: " + bin.getSize() +
						"\nfirst byte: " + bin.getStream().read());
			}
		}
	}

	/**
	 * @param in
	 */
	private void internalizeBinaries(Node in, Session session)
			throws RepositoryException {
		for (NodeIterator iter = in.getNodes(); iter.hasNext();) {
			Node f = (Node) iter.next();
			if (f.isNodeType("nt:file")) {
				Node content = f.getNode("jcr:content");
				Binary bin = content.getProperty("jcr:data").getBinary();
				Binary newbin =
						session.getValueFactory().createBinary(bin.getStream());
				content.setProperty("jcr:data", newbin);
			} else if (f.isNodeType("nt:folder")) {
				internalizeBinaries(f, session);
			}

		}
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

}
