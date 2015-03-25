package de.uniko.sebschlicht.graphity.neo4j.model;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import de.uniko.sebschlicht.graphity.neo4j.EdgeType;
import de.uniko.sebschlicht.graphity.neo4j.Walker;

/**
 * node proxy for an user that can act in the social network
 * 
 * @author sebschlicht
 * 
 */
public class UserProxy extends SocialNodeProxy {

    /**
     * unique user identifier
     */
    public static final String PROP_IDENTIFIER = "identifier";

    /**
     * timestamp of last stream update
     */
    public static final String PROP_LAST_STREAM_UDPATE = "stream_update";

    /**
     * (optional) last recent status update posted by this user
     */
    protected StatusUpdateProxy lastPost;

    protected String identifier;

    /**
     * (optional) timestamp of the last recent status update posted by this user
     */
    protected long _lastPostTimestamp;

    /**
     * Create a user node proxy to provide data access and manipulation.
     * 
     * @param nUser
     *            user node to get and set data
     */
    public UserProxy(
            Node nUser) {
        super(nUser);
        _lastPostTimestamp = -1;
    }

    /**
     * Adds a status update to the user.<br>
     * Links the status update node to the user node and to previous updates
     * if any.
     * Updates the author node's last post timestamp.
     * 
     * @param pStatusUpdate
     *            proxy of the new status update
     */
    public void addStatusUpdate(StatusUpdateProxy pStatusUpdate) {
        linkStatusUpdate(pStatusUpdate);
        // update last post timestamp
        setLastPostTimestamp(pStatusUpdate.getPublished());
    }

    /**
     * Links a status update node to the user node and to previous updates if
     * any.
     * 
     * @param pStatusUpdate
     *            proxy of the status update
     */
    public void linkStatusUpdate(StatusUpdateProxy pStatusUpdate) {
        // get last recent status update
        Node lastUpdate = Walker.nextNode(node, EdgeType.PUBLISHED);
        // update references to previous status update (if existing)
        if (lastUpdate != null) {
            node.getSingleRelationship(EdgeType.PUBLISHED, Direction.OUTGOING)
                    .delete();
            pStatusUpdate.getNode().createRelationshipTo(lastUpdate,
                    EdgeType.PUBLISHED);
        }
        // add reference from user to current update node
        node.createRelationshipTo(pStatusUpdate.getNode(), EdgeType.PUBLISHED);
    }

    /**
     * Retrieves the user identifier.<br>
     * Caches the user identifier for future calls.
     * 
     * @return (cached) user identifier
     */
    public String getIdentifier() {
        if (identifier == null) {
            identifier = (String) node.getProperty(PROP_IDENTIFIER);
        }
        return identifier;
    }

    public void setLastPostTimestamp(long lastPostTimestamp) {
        node.setProperty(PROP_LAST_STREAM_UDPATE, lastPostTimestamp);
        _lastPostTimestamp = lastPostTimestamp;
    }

    public long getLastPostTimestamp() {
        if (_lastPostTimestamp == -1) {
            _lastPostTimestamp =
                    (long) node.getProperty(PROP_LAST_STREAM_UDPATE, 0L);
        }
        return _lastPostTimestamp;
    }
}
