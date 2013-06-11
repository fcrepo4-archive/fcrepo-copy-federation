package org.fcrepo.sequencer.copy;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Session;

import org.modeshape.jcr.api.sequencer.Sequencer;


/**
 * Makes a copy of the node and subtree referenced by a property to the configured output node. This implementation works within or across Workspaces.
 * @author Gregory Jansen
 *
 */
public class NodeCopySequencer extends Sequencer {


	/* (non-Javadoc)
	 * @see org.modeshape.jcr.api.sequencer.Sequencer#execute(javax.jcr.Property, javax.jcr.Node, org.modeshape.jcr.api.sequencer.Sequencer.Context)
	 */
	@Override
	public boolean execute(Property inputProperty, Node outputNode,
			Context context) throws Exception {
		getLogger().debug("Execute called with {} {} {}", inputProperty, outputNode, context);
		String srcWS = inputProperty.getParent().getSession().getWorkspace().getName();
		Session session = outputNode.getSession();
		session.getWorkspace().copy(srcWS, inputProperty.getParent().getPath(), outputNode.getPath()+"/"+inputProperty.getParent().getName());
		return true;  // no need to save session, return true to save changes
	}

}
