package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.io.File;
import java.util.TreeSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import de.uniko.sebschlicht.graphity.exception.IllegalUserIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownReaderIdException;
import de.uniko.sebschlicht.graphity.neo4j.EdgeType;
import de.uniko.sebschlicht.graphity.neo4j.Neo4jGraphity;
import de.uniko.sebschlicht.graphity.neo4j.NodeType;
import de.uniko.sebschlicht.graphity.neo4j.Walker;
import de.uniko.sebschlicht.graphity.neo4j.model.PostIteratorComparator;
import de.uniko.sebschlicht.graphity.neo4j.model.StatusUpdateProxy;
import de.uniko.sebschlicht.graphity.neo4j.model.UserPostIterator;
import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;
import de.uniko.sebschlicht.socialnet.StatusUpdate;
import de.uniko.sebschlicht.socialnet.StatusUpdateList;

/**
 * Graphity implementation optimized for read requests
 * 
 * @author Rene Pickhardt, Jonas Kunze, sebschlicht
 * 
 */
public class ReadOptimizedGraphity extends Neo4jGraphity {

    public ReadOptimizedGraphity(
            GraphDatabaseService graphDb) {
        super(graphDb);
    }

    @Override
    protected boolean addFollowship(UserProxy following, UserProxy followed) {
        // try to find the replica node of the user followed
        Node rFollowed = null;
        for (Relationship followship : following.getNode().getRelationships(
                EdgeType.FOLLOWS, Direction.OUTGOING)) {
            rFollowed = followship.getEndNode();
            if (Walker.nextNode(rFollowed, EdgeType.REPLICA).equals(
                    followed.getNode())) {
                // user is following already
                return false;
            }
        }

        // create replica
        final Node newReplica = graphDb.createNode();
        following.getNode().createRelationshipTo(newReplica, EdgeType.FOLLOWS);
        newReplica.createRelationshipTo(followed.getNode(), EdgeType.REPLICA);

        // check if followed user is the first in following's ego network
        if (Walker.nextNode(following.getNode(), EdgeType.GRAPHITY) == null) {
            following.getNode().createRelationshipTo(newReplica,
                    EdgeType.GRAPHITY);
        } else {
            // search for insertion index within following replica layer
            final long followedTimestamp = getLastUpdateByReplica(newReplica);
            long crrTimestamp;
            Node prev = following.getNode();
            Node next = null;
            while (true) {
                // get next user
                next = Walker.nextNode(prev, EdgeType.GRAPHITY);
                if (next != null) {
                    crrTimestamp = getLastUpdateByReplica(next);
                    // step on if current user has newer status updates
                    if (crrTimestamp > followedTimestamp) {
                        prev = next;
                        continue;
                    }
                }
                // insertion position has been found
                break;
            }
            // insert followed user's replica into following's ego network
            if (next != null) {
                prev.getSingleRelationship(EdgeType.GRAPHITY,
                        Direction.OUTGOING).delete();
                newReplica.createRelationshipTo(next, EdgeType.GRAPHITY);
            }
            prev.createRelationshipTo(newReplica, EdgeType.GRAPHITY);
        }
        return true;
    }

    /**
     * remove a followed user from the replica layer
     * 
     * @param rFollowed
     *            replica of the user that will be removed
     */
    private void removeFromReplicaLayer(final Node rFollowed) {
        final Node prev = Walker.previousNode(rFollowed, EdgeType.GRAPHITY);
        final Node next = Walker.nextNode(rFollowed, EdgeType.GRAPHITY);
        // bridge the user replica in the replica layer
        prev.getSingleRelationship(EdgeType.GRAPHITY, Direction.OUTGOING)
                .delete();
        if (next != null) {
            next.getSingleRelationship(EdgeType.GRAPHITY, Direction.INCOMING)
                    .delete();
            prev.createRelationshipTo(next, EdgeType.GRAPHITY);
        }
        // remove the followship
        rFollowed.getSingleRelationship(EdgeType.FOLLOWS, Direction.INCOMING)
                .delete();
        // remove the replica node itself
        rFollowed.getSingleRelationship(EdgeType.REPLICA, Direction.OUTGOING)
                .delete();
        rFollowed.delete();
    }

    @Override
    protected boolean removeFollowship(UserProxy following, UserProxy followed) {
        // find the replica node of the user followed
        Node rFollowed = null;
        for (Relationship followship : following.getNode().getRelationships(
                EdgeType.FOLLOWS, Direction.OUTGOING)) {
            rFollowed = followship.getEndNode();
            if (Walker.nextNode(rFollowed, EdgeType.REPLICA).equals(
                    followed.getNode())) {
                break;
            }
            rFollowed = null;
        }
        // there is no such followship existing
        if (rFollowed == null) {
            return false;
        }
        removeFromReplicaLayer(rFollowed);
        return true;
    }

    /**
     * update the ego networks of a user's followers
     * 
     * @param nUser
     *            user where changes have occurred
     */
    private void updateEgoNetworks(final Node nUser) {
        Node rFollowed, following;
        Node prev, next, last;
        // loop through followers
        for (Relationship relationship : nUser.getRelationships(
                EdgeType.REPLICA, Direction.INCOMING)) {
            // load each replica and the user corresponding
            rFollowed = relationship.getStartNode();
            following = Walker.previousNode(rFollowed, EdgeType.FOLLOWS);
            // bridge user node
            prev = Walker.previousNode(rFollowed, EdgeType.GRAPHITY);
            if (!prev.equals(following)) {
                rFollowed.getSingleRelationship(EdgeType.GRAPHITY,
                        Direction.INCOMING).delete();
                next = Walker.nextNode(rFollowed, EdgeType.GRAPHITY);
                if (next != null) {
                    rFollowed.getSingleRelationship(EdgeType.GRAPHITY,
                            Direction.OUTGOING).delete();
                    prev.createRelationshipTo(next, EdgeType.GRAPHITY);
                }
            }
            // insert user's replica at its new position
            last = Walker.nextNode(following, EdgeType.GRAPHITY);
            if (!last.equals(rFollowed)) {
                following.getSingleRelationship(EdgeType.GRAPHITY,
                        Direction.OUTGOING).delete();
                following.createRelationshipTo(rFollowed, EdgeType.GRAPHITY);
                rFollowed.createRelationshipTo(last, EdgeType.GRAPHITY);
            }
        }
    }

    @Override
    protected long addStatusUpdate(
            Node nAuthor,
            StatusUpdate statusUpdate,
            Transaction tx) {
        // lock user and ego network
        TreeSet<UserProxy> subscribers =
                new TreeSet<>(new LockUserComparator());
        subscribers.add(new UserProxy(nAuthor));
        Node followingReplica, followingUser;
        for (Relationship followship : nAuthor.getRelationships(
                EdgeType.REPLICA, Direction.INCOMING)) {
            followingReplica = followship.getStartNode();
            followingUser =
                    Walker.previousNode(followingReplica, EdgeType.FOLLOWS);
            subscribers.add(new UserProxy(followingUser));
        }
        for (UserProxy user : subscribers) {
            tx.acquireWriteLock(user.getNode());
        }

        return addStatusUpdate(nAuthor, statusUpdate);
        //TODO release locks
    }

    @Override
    protected long addStatusUpdate(Node nAuthor, StatusUpdate statusUpdate) {
        // create new status update node and fill via proxy
        Node crrUpdate = graphDb.createNode(NodeType.UPDATE);
        StatusUpdateProxy pStatusUpdate = new StatusUpdateProxy(crrUpdate);
        //TODO handle service overload
        pStatusUpdate.initNode(statusUpdate.getPublished(),
                statusUpdate.getMessage());

        // add status update to user (link node, update user)
        UserProxy pAuthor = new UserProxy(nAuthor);
        pAuthor.addStatusUpdate(pStatusUpdate);

        // update ego networks of status update author followers
        updateEgoNetworks(nAuthor);

        return pStatusUpdate.getIdentifier();
    }

    @Override
    protected StatusUpdateList readStatusUpdates(
            Node nReader,
            int numStatusUpdates) {
        StatusUpdateList statusUpdates = new StatusUpdateList();
        final TreeSet<UserPostIterator> postIterators =
                new TreeSet<UserPostIterator>(new PostIteratorComparator());

        // load first user by replica
        UserProxy pCrrUser = null;
        UserPostIterator userPostIterator;
        Node nReplica = Walker.nextNode(nReader, EdgeType.GRAPHITY);
        if (nReplica != null) {
            pCrrUser =
                    new UserProxy(Walker.nextNode(nReplica, EdgeType.REPLICA));
            userPostIterator = new UserPostIterator(pCrrUser);
            userPostIterator.setReplicaNode(nReplica);

            if (userPostIterator.hasNext()) {
                postIterators.add(userPostIterator);
            }
        }

        // handle user queue
        UserProxy pPrevUser = pCrrUser;
        while (statusUpdates.size() < numStatusUpdates
                && !postIterators.isEmpty()) {
            // add last recent status update
            userPostIterator = postIterators.pollLast();
            statusUpdates.add(userPostIterator.next().getStatusUpdate());

            // re-add iterator if not empty
            if (userPostIterator.hasNext()) {
                postIterators.add(userPostIterator);
            }

            // load additional user if necessary
            if (userPostIterator.getUser() == pPrevUser) {
                nReplica =
                        Walker.nextNode(userPostIterator.getReplicaNode(),
                                EdgeType.GRAPHITY);
                // check if additional user existing
                if (nReplica != null) {
                    pCrrUser =
                            new UserProxy(Walker.nextNode(nReplica,
                                    EdgeType.REPLICA));
                    userPostIterator = new UserPostIterator(pCrrUser);
                    userPostIterator.setReplicaNode(nReplica);
                    // check if user has status updates
                    if (userPostIterator.hasNext()) {
                        postIterators.add(userPostIterator);
                        pPrevUser = pCrrUser;
                    } else {
                        // further users do not need to be loaded
                        pPrevUser = null;
                    }
                }
            }
        }

        //            // access single stream only
        //            final UserProxy posterNode = new UserProxy(nReader);
        //            UserPostIterator postIterator = new UserPostIterator(posterNode);
        //
        //            while ((statusUpdates.size() < numStatusUpdates)
        //                    && postIterator.hasNext()) {
        //                statusUpdates.add(postIterator.next().getStatusUpdate());
        //            }

        return statusUpdates;
    }

    /**
     * Retrieves the timestamp of the last recent status update of the user
     * specified.
     * 
     * @param userReplica
     *            replica of the user
     * @return timestamp of the user's last recent status update
     */
    private static long getLastUpdateByReplica(final Node userReplica) {
        final Node user = Walker.nextNode(userReplica, EdgeType.REPLICA);
        UserProxy pUser = new UserProxy(user);
        return pUser.getLastPostTimestamp();
    }

    public static void main(String[] args) throws Exception {
        GraphDatabaseBuilder builder =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
                        new File("/tmp/testdb").getAbsolutePath()).setConfig(
                        GraphDatabaseSettings.cache_type, "none");
        GraphDatabaseService graphDb = builder.newGraphDatabase();
        Neo4jGraphity graphity = new ReadOptimizedGraphity(graphDb);
        try {
            System.out.println(graphity.addFollowship("1", "2"));
            System.out.println(graphity.addFollowship("1", "3"));
            System.out.println(graphity.addFollowship("1", "4"));

            System.out.println(graphity.addFollowship("2", "1"));
            System.out.println(graphity.addFollowship("2", "4"));

            System.out.println(graphity.addStatusUpdate("4", "mine"));
            System.out.println(graphity.addStatusUpdate("4", "of"));
            System.out.println(graphity.addStatusUpdate("3", "friend"));
            System.out.println(graphity.addStatusUpdate("2", "dear"));
            System.out.println(graphity.addStatusUpdate("2", "my"));
            System.out.println(graphity.addStatusUpdate("3", "hello"));

            System.out.println("-------");
            System.out.println(graphity.readStatusUpdates("1", 15));
            System.out.println("-------");
            System.out.println(graphity.removeFollowship("1", "2"));
            System.out.println(graphity.readStatusUpdates("1", 15));
            System.out.println("-------");
            System.out.println(graphity.readStatusUpdates("2", 2));
            System.out.println("-------");
            System.out.println(graphity.readStatusUpdates("2", 1));
            if (graphity.readStatusUpdates("3", 10).size() == 0) {
                System.out.println("...");
            }
        } catch (IllegalUserIdException | UnknownReaderIdException e) {
            e.printStackTrace();
        }
        graphDb.shutdown();
    }
}
