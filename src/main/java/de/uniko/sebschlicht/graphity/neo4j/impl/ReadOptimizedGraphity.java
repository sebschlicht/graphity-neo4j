package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import de.uniko.sebschlicht.graphity.exception.IllegalUserIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownFollowedIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownFollowingIdException;
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
    public boolean addFollowship(String sIdFollowing, String sIdFollowed)
            throws IllegalUserIdException {
        long idFollowing = checkUserId(sIdFollowing);
        long idFollowed = checkUserId(sIdFollowed);
        try (Transaction tx = graphDb.beginTx()) {
            UserProxy following = loadUser(idFollowing);
            UserProxy followed = loadUser(idFollowed);

            Lock[] locks = LockManager.lock(tx, following, followed);
            boolean result = addFollowship(following, followed);
            //LockManager.releaseLocks(locks);

            if (!result) {
                System.out.println("rollback!");
                return false;
            }

            long timestamp = System.currentTimeMillis();
            //addStatusUpdate(following, new StatusUpdate(sIdFollowing,
            //        timestamp, "now follows " + sIdFollowed), tx);

            //addStatusUpdate(followed, new StatusUpdate(sIdFollowed, timestamp,
            //        "has new follower " + sIdFollowing), tx);
            tx.success();
            System.out.println("commit.");
        }
        addStatusUpdate(sIdFollowing, "now follows " + sIdFollowed);
        addStatusUpdate(sIdFollowed, "has new follower " + sIdFollowing);
        return true;
    }

    @Override
    protected boolean addFollowship(UserProxy following, UserProxy followed) {
        // try to find the followed user
        Node nFollowed = null;
        for (Relationship followship : following.getNode().getRelationships(
                EdgeType.FOLLOWS, Direction.OUTGOING)) {
            nFollowed = followship.getEndNode();
            if (followed.getNode().equals(nFollowed)) {
                // user is following already
                return false;
            }
        }
        following.getNode().createRelationshipTo(followed.getNode(),
                EdgeType.FOLLOWS);
        System.out.println(following.getNode() + " -> FOLLOWS -> "
                + followed.getNode());

        // insert the followed user into the ego network of the subscriber
        insertIntoEgoNetwork(following, followed.getNode());
        return true;
    }

    /**
     * Inserts a user in the ego network of a subscribing user.<br>
     * After the insertion the user node will have the correct position in
     * the ego network of the user, according to the Graphity index.
     * 
     * @param nFollowing
     *            subscriber node
     * @param nFollowed
     *            followed user node
     */
    protected void insertIntoEgoNetwork(UserProxy following, Node nFollowed) {
        RelationshipType graphity =
                graphityIndexType(following.getIdentifier());
        StringBuilder dm = new StringBuilder();
        Node t = following.getNode();
        dm.append("GRAPHITY:");
        dm.append(following.getIdentifier());
        dm.append(" ");
        dm.append(t);
        do {
            t = Walker.nextNode(t, graphity);
            if (t != null) {
                dm.append(" -> ");
                dm.append(t);
            }
        } while (t != null);
        System.out.println("[before] " + dm);

        Node prev = following.getNode();
        Node next = null;
        long insertionTimestamp = getLastUpdate(nFollowed);
        long timestamp;
        do {
            next = Walker.nextNode(prev, graphity);
            if (next != null) {
                System.out.println("compare user updates...");
                timestamp = getLastUpdate(next);
                if (timestamp > insertionTimestamp) {
                    System.out.println(next + " is more recent.");
                    // current user has more recent news items, step on
                    prev = next;
                    continue;
                }
                System.out.println(nFollowed + " is as recent or higher!");
            }
            // no next user or less recent news items -> insertion position found
            break;
        } while (next != null);

        // if there is a tail, connect it to the new user
        if (next != null) {
            prev.getSingleRelationship(graphity, Direction.OUTGOING).delete();
            nFollowed.createRelationshipTo(next, graphity);
        }
        // connect the new user to the index head (subscriber or user with more recent news items)
        prev.createRelationshipTo(nFollowed, graphity);

        dm = new StringBuilder();
        t = following.getNode();
        dm.append("GRAPHITY:");
        dm.append(following.getIdentifier());
        dm.append(" ");
        dm.append(t);
        do {
            t = Walker.nextNode(t, graphity);
            if (t != null) {
                dm.append(" -> ");
                dm.append(t);
            }
        } while (t != null);
        System.out.println("[after] " + dm);
    }

    @Override
    public boolean removeFollowship(String sIdFollowing, String sIdFollowed)
            throws IllegalUserIdException, UnknownFollowingIdException,
            UnknownFollowedIdException {
        long idFollowing = checkUserId(sIdFollowing);
        long idFollowed = checkUserId(sIdFollowed);
        try (Transaction tx = graphDb.beginTx()) {
            UserProxy following = findUser(idFollowing);
            if (following == null) {
                throw new UnknownFollowingIdException(sIdFollowing);
            }
            UserProxy followed = findUser(idFollowed);
            if (followed == null) {
                throw new UnknownFollowedIdException(sIdFollowed);
            }

            Lock[] locks = LockManager.lock(tx, following, followed);
            boolean result = removeFollowship(following, followed);
            //LockManager.releaseLocks(locks);

            if (!result) {
                return false;
            }
            tx.success();
        }
        addStatusUpdate(sIdFollowing, "did unfollow " + sIdFollowed);
        addStatusUpdate(sIdFollowed, "was unfollowed by " + sIdFollowing);
        return true;
    }

    @Override
    protected boolean removeFollowship(UserProxy following, UserProxy followed) {
        // delete the followship if existing
        Relationship followship = null;
        for (Relationship follows : following.getNode().getRelationships(
                Direction.OUTGOING, EdgeType.FOLLOWS)) {
            if (follows.getEndNode().equals(followed.getNode())) {
                followship = follows;
                break;
            }
        }

        // there is no such followship existing
        if (followship == null) {
            return false;
        }

        // remove the followship
        removeFromEgoNetwork(following, followed.getNode());
        followship.delete();
        return true;
    }

    /**
     * Removes a followed user from the ego network of a user.
     * 
     * @param following
     *            subscriber
     * @param nFollowed
     *            user node that will be removed from the Graphity index
     */
    private void removeFromEgoNetwork(UserProxy following, Node nFollowed) {
        RelationshipType graphity =
                graphityIndexType(following.getIdentifier());

        final Node prev = Walker.previousNode(nFollowed, graphity);
        final Node next = Walker.nextNode(nFollowed, graphity);
        // bridge the user node
        prev.getSingleRelationship(graphity, Direction.OUTGOING).delete();
        if (next != null) {
            next.getSingleRelationship(graphity, Direction.INCOMING).delete();
            prev.createRelationshipTo(next, graphity);
        }
    }

    @Override
    protected long addStatusUpdate(
            UserProxy author,
            StatusUpdate statusUpdate,
            Transaction tx) {
        // lock user and ego network
        List<UserProxy> replicaLayer = new LinkedList<>();
        replicaLayer.add(author);
        Node following;
        for (Relationship followship : author.getNode().getRelationships(
                EdgeType.FOLLOWS, Direction.INCOMING)) {
            following = followship.getStartNode();
            replicaLayer.add(new UserProxy(following));
        }
        List<Lock> locks = LockManager.lock(tx, replicaLayer);
        try {
            return addStatusUpdate(author, statusUpdate);
        } finally {
            //LockManager.releaseLocks(locks);
        }
    }

    @Override
    protected long addStatusUpdate(UserProxy author, StatusUpdate statusUpdate) {
        // create new status update node and fill via proxy
        Node crrUpdate = graphDb.createNode(NodeType.UPDATE);
        StatusUpdateProxy pStatusUpdate = new StatusUpdateProxy(crrUpdate);
        //TODO handle service overload
        pStatusUpdate.initNode(statusUpdate.getPublished(),
                statusUpdate.getMessage());

        // add status update to user (link node, update user)
        author.addStatusUpdate(pStatusUpdate);

        // update ego networks of status update author followers
        updateEgoNetworks(author);
        return pStatusUpdate.getIdentifier();
    }

    /**
     * update the ego networks of a user's followers
     * 
     * @param nUser
     *            user where changes have occurred
     */
    private void updateEgoNetworks(final UserProxy user) {
        Node nAuthor = user.getNode(), nFollower;
        Node prev, next, lastRecent;
        RelationshipType graphity;

        // loop through followers
        for (Relationship relationship : nAuthor.getRelationships(
                EdgeType.FOLLOWS, Direction.INCOMING)) {
            // load each follower
            nFollower = relationship.getStartNode();
            graphity =
                    graphityIndexType(new UserProxy(nFollower).getIdentifier());

            StringBuilder dm = new StringBuilder();
            Node t = nFollower;
            dm.append("GRAPHITY:");
            dm.append(new UserProxy(nFollower).getIdentifier());
            dm.append(" ");
            dm.append(t);
            do {
                t = Walker.nextNode(t, graphity);
                if (t != null) {
                    dm.append(" -> ");
                    dm.append(t);
                }
            } while (t != null);
            System.out.println("[before] " + dm);

            // ensure author is head of followers Graphity index
            prev = Walker.previousNode(nAuthor, graphity);
            if (!prev.equals(nFollower)) {
                lastRecent = Walker.nextNode(nFollower, graphity);
                next = Walker.nextNode(nAuthor, graphity);

                // make author the head of the Graphity index
                nFollower.getSingleRelationship(graphity, Direction.OUTGOING)
                        .delete();
                nAuthor.getSingleRelationship(graphity, Direction.INCOMING)
                        .delete();
                nFollower.createRelationshipTo(nAuthor, graphity);

                // bridge author
                if (next != null) {
                    nAuthor.getSingleRelationship(graphity, Direction.OUTGOING)
                            .delete();
                    prev.createRelationshipTo(next, graphity);
                }
                nAuthor.createRelationshipTo(lastRecent, graphity);
            }

            dm = new StringBuilder();
            t = nFollower;
            dm.append("GRAPHITY:");
            dm.append(new UserProxy(nFollower).getIdentifier());
            dm.append(" ");
            dm.append(t);
            do {
                t = Walker.nextNode(t, graphity);
                if (t != null) {
                    dm.append(" -> ");
                    dm.append(t);
                }
            } while (t != null);
            System.out.println("[after] " + dm);
        }
    }

    @Override
    protected StatusUpdateList readStatusUpdates(
            UserProxy reader,
            int numStatusUpdates) {
        StatusUpdateList statusUpdates = new StatusUpdateList();
        final TreeSet<UserPostIterator> postIterators =
                new TreeSet<UserPostIterator>(new PostIteratorComparator());
        RelationshipType graphity = graphityIndexType(reader.getIdentifier());

        // load first user
        UserProxy pCrrUser = null;
        UserPostIterator userPostIterator;
        Node nSubscribed = Walker.nextNode(reader.getNode(), graphity);
        if (nSubscribed != null) {
            pCrrUser = new UserProxy(nSubscribed);
            userPostIterator = new UserPostIterator(pCrrUser);

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
                nSubscribed =
                        Walker.nextNode(userPostIterator.getUser().getNode(),
                                graphity);
                // check if additional user existing
                if (nSubscribed != null) {
                    pCrrUser = new UserProxy(nSubscribed);
                    userPostIterator = new UserPostIterator(pCrrUser);
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
     * @param nUser
     *            user node
     * @return timestamp of the user's last recent status update
     */
    private static long getLastUpdate(final Node nUser) {
        UserProxy user = new UserProxy(nUser);
        return user.getLastPostTimestamp();
    }

    public static RelationshipType graphityIndexType(long id) {
        return DynamicRelationshipType.withName("graphity:" + id);
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
