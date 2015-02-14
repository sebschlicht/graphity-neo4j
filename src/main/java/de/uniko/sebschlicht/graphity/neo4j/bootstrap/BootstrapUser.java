package de.uniko.sebschlicht.graphity.neo4j.bootstrap;

import java.util.ArrayList;
import java.util.Random;

public class BootstrapUser {

    private static final Random RANDOM = new Random();

    private final long _id;

    private long _nodeId;

    private ArrayList<Long> _subscriptions;

    private int _numStatusUpdates;

    private long[] _statusUpdateNodeIds;

    public BootstrapUser(
            long id) {
        _id = id;
        _subscriptions = null;
        _numStatusUpdates = 0;
        _statusUpdateNodeIds = null;
    }

    public long getId() {
        return _id;
    }

    public void setNodeId(long nodeId) {
        _nodeId = nodeId;
    }

    public long getNodeId() {
        return _nodeId;
    }

    public void addSubscription(long idFollowed) {
        if (_subscriptions == null) {
            _subscriptions = new ArrayList<Long>(3);
        }
        _subscriptions.add(idFollowed);
    }

    public ArrayList<Long> getSubscriptions() {
        return _subscriptions;
    }

    public long getRandomSubscription() {
        return _subscriptions.get(RANDOM.nextInt(_subscriptions.size()));
    }

    public void addStatusUpdate() {
        _numStatusUpdates += 1;
    }

    public int getNumStatusUpdates() {
        return _numStatusUpdates;
    }

    public void setPostNodeIds(long[] postNodeIds) {
        _statusUpdateNodeIds = postNodeIds;
    }

    public long[] getPostNodeIds() {
        return _statusUpdateNodeIds;
    }
}
