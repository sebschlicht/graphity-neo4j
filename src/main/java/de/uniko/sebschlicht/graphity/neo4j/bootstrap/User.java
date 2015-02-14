package de.uniko.sebschlicht.graphity.neo4j.bootstrap;

public class User {

    /**
     * these are only valid as long as no node was deleted!
     */
    private long _nodeId;

    private User[] _subscriptions;

    private long[] _postNodeIds;

    public User() {
        _subscriptions = null;
        _postNodeIds = null;
    }

    public void setNodeId(long nodeId) {
        _nodeId = nodeId;
    }

    public long getNodeId() {
        return _nodeId;
    }

    public void setSubscriptions(User[] subscriptions) {
        _subscriptions = subscriptions;
    }

    public User[] getSubscriptions() {
        return _subscriptions;
    }

    public void setPostNodeIds(long[] postNodeIds) {
        _postNodeIds = postNodeIds;
    }

    public long[] getPostNodeIds() {
        return _postNodeIds;
    }
}
