package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.io.File;
import java.util.TreeSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
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
import de.uniko.sebschlicht.graphity.neo4j.model.PostIteratorComparator;
import de.uniko.sebschlicht.graphity.neo4j.model.StatusUpdateProxy;
import de.uniko.sebschlicht.graphity.neo4j.model.UserPostIterator;
import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;
import de.uniko.sebschlicht.socialnet.StatusUpdate;
import de.uniko.sebschlicht.socialnet.StatusUpdateList;

/**
 * Graphity implementation optimized for write requests
 * 
 * @author Rene Pickhardt, Jonas Kunze, sebschlicht
 * 
 */
public class WriteOptimizedGraphity extends Neo4jGraphity {

    public WriteOptimizedGraphity(
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
            LockManager.releaseLocks(locks);

            if (result) {
                long msCrr = System.currentTimeMillis();
                addStatusUpdate(following, new StatusUpdate(sIdFollowing,
                        msCrr, "now follows " + sIdFollowed), tx);
                addStatusUpdate(followed, new StatusUpdate(sIdFollowed,
                        msCrr + 1, "has new follower " + sIdFollowing), tx);
                tx.success();
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean addFollowship(UserProxy following, UserProxy followed) {
        // try to find the node of the user followed
        for (Relationship followship : following.getNode().getRelationships(
                EdgeType.FOLLOWS, Direction.OUTGOING)) {
            if (followship.getEndNode().equals(followed.getNode())) {
                return false;
            }
        }
        // create star topology
        following.getNode().createRelationshipTo(followed.getNode(),
                EdgeType.FOLLOWS);
        return true;
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
            LockManager.releaseLocks(locks);

            if (result) {
                long msCrr = System.currentTimeMillis();
                addStatusUpdate(followed, new StatusUpdate(sIdFollowed, msCrr,
                        "was unfollowed by " + sIdFollowing), tx);
                addStatusUpdate(following, new StatusUpdate(sIdFollowing,
                        msCrr + 1, "did unfollow " + sIdFollowed), tx);
                tx.success();
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean removeFollowship(UserProxy following, UserProxy followed) {
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

        followship.delete();
        return true;
    }

    @Override
    protected long addStatusUpdate(
            UserProxy author,
            StatusUpdate statusUpdate,
            Transaction tx) {
        // lock user
        Lock lock = LockManager.lock(tx, author);
        try {
            return addStatusUpdate(author, statusUpdate);
        } finally {
            lock.release();
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

        return pStatusUpdate.getIdentifier();
    }

    @Override
    protected StatusUpdateList readStatusUpdates(
            UserProxy reader,
            int numStatusUpdates) {
        StatusUpdateList statusUpdates = new StatusUpdateList();
        final TreeSet<UserPostIterator> postIterators =
                new TreeSet<UserPostIterator>(new PostIteratorComparator());

        // loop through users followed
        UserProxy pCrrUser;
        UserPostIterator userPostIterator;
        for (Relationship relationship : reader.getNode().getRelationships(
                EdgeType.FOLLOWS, Direction.OUTGOING)) {
            // add post iterator
            pCrrUser = new UserProxy(relationship.getEndNode());
            userPostIterator = new UserPostIterator(pCrrUser);

            if (userPostIterator.hasNext()) {
                postIterators.add(userPostIterator);
            }
        }

        // handle queue
        while ((statusUpdates.size() < numStatusUpdates)
                && !postIterators.isEmpty()) {
            // add last recent status update
            userPostIterator = postIterators.pollLast();
            statusUpdates.add(userPostIterator.next().getStatusUpdate());

            // re-add iterator if not empty
            if (userPostIterator.hasNext()) {
                postIterators.add(userPostIterator);
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

    public static void main(String[] args) {
        GraphDatabaseBuilder builder =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
                        new File("/tmp/testdb").getAbsolutePath()).setConfig(
                        GraphDatabaseSettings.cache_type, "none");
        GraphDatabaseService graphDb = builder.newGraphDatabase();
        Neo4jGraphity graphity = new WriteOptimizedGraphity(graphDb);
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
