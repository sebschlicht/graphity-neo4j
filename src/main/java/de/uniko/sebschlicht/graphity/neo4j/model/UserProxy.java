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
     * unique user identifier
     */
    protected long _identifier;

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
        _identifier = -1;
        _lastPostTimestamp = -1;
    }

    /**
     * Create a user node proxy to provide data access and manipulation.
     * 
     * @param nUser
     *            user node to get and set data
     * @param identifier
     *            user identifier to avoid its lookup
     */
    public UserProxy(
            Node nUser,
            long identifier) {
        this(nUser);
        _identifier = identifier;
    }

    /**
     * Caches the identifier in order to avoid in-graph lookups.
     * 
     * @param identifier
     *            user identifier
     */
    public void cacheIdentifier(long identifier) {
        _identifier = identifier;
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
        Node lastUpdate = Walker.nextNode(_node, EdgeType.PUBLISHED);
        // update references to previous status update (if existing)
        if (lastUpdate != null) {
            _node.getSingleRelationship(EdgeType.PUBLISHED, Direction.OUTGOING)
                    .delete();
            pStatusUpdate.getNode().createRelationshipTo(lastUpdate,
                    EdgeType.PUBLISHED);
        }
        // add reference from user to current update node
        _node.createRelationshipTo(pStatusUpdate.getNode(), EdgeType.PUBLISHED);
    }

    /**
     * Retrieves the user identifier.<br>
     * Caches the user identifier for future calls.
     * 
     * @return (cached) user identifier
     */
    public long getIdentifier() {
        if (_identifier == -1) {
            _identifier = (long) _node.getProperty(PROP_IDENTIFIER);
        }
        return _identifier;
    }

    public void setLastPostTimestamp(long lastPostTimestamp) {
        _node.setProperty(PROP_LAST_STREAM_UDPATE, lastPostTimestamp);
        _lastPostTimestamp = lastPostTimestamp;
    }

    /**
     * @return (optional) timestamp of the last recent status update posted by
     *         this user<br>
     *         defaults to <code>0</code>
     */
    public long getLastPostTimestamp() {
        if (_lastPostTimestamp == -1) {
            _lastPostTimestamp =
                    (long) _node.getProperty(PROP_LAST_STREAM_UDPATE, 0L);
        }
        return _lastPostTimestamp;
    }
}
