package de.uniko.sebschlicht.graphity.neo4j;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;

import de.uniko.sebschlicht.graphity.Graphity;
import de.uniko.sebschlicht.graphity.exception.IllegalUserIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownFollowedIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownFollowingIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownReaderIdException;
import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;
import de.uniko.sebschlicht.socialnet.StatusUpdate;
import de.uniko.sebschlicht.socialnet.StatusUpdateList;

/**
 * social graph for Graphity implementations
 * 
 * @author sebschlicht
 * 
 */
public abstract class Neo4jGraphity extends Graphity {

    /**
     * graph database holding the social network graph
     */
    protected GraphDatabaseService graphDb;

    /**
     * Creates a new Graphity instance using the Neo4j database provided.
     * 
     * @param graphDb
     *            graph database holding any Graphity social network graph to
     *            operate on
     */
    public Neo4jGraphity(
            GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    public Transaction beginTx() {
        return graphDb.beginTx();
    }

    @Override
    public void init() {
        // create user identifier index if not existing
        IndexDefinition indexUserId =
                loadIndexDefinition(NodeType.USER, UserProxy.PROP_IDENTIFIER);
        if (indexUserId == null) {
            try (Transaction tx = graphDb.beginTx()) {
                graphDb.schema().indexFor(NodeType.USER)
                        .on(UserProxy.PROP_IDENTIFIER).create();
                tx.success();
            }
        }

        try (Transaction tx = graphDb.beginTx()) {
            graphDb.schema().awaitIndexesOnline(60, TimeUnit.SECONDS);
        }
    }

    /**
     * Loads the index definition for a label on a certain property key.
     * 
     * @param label
     *            label the index was created for
     * @param propertyKey
     *            property key the index was created on
     * @return index definition - for the label on the property specified<br>
     *         <b>null</b> - if there is no index for the label on this property
     */
    protected IndexDefinition loadIndexDefinition(
            Label label,
            String propertyKey) {
        try (Transaction tx = graphDb.beginTx()) {
            for (IndexDefinition indexDefinition : graphDb.schema().getIndexes(
                    label)) {
                for (String indexPropertyKey : indexDefinition
                        .getPropertyKeys()) {
                    if (indexPropertyKey.equals(propertyKey)) {
                        return indexDefinition;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Creates a user that can act in the social network.
     * 
     * @param userIdentifier
     *            identifier of the new user
     * @return user node - if the user was successfully created<br>
     *         <b>null</b> - if the identifier is already in use
     * @throws IllegalUserIdException
     *             if the user identifier is invalid
     */
    public Node createUser(String userIdentifier) throws IllegalUserIdException {
        try {
            long idUser = Long.valueOf(userIdentifier);
            if (idUser > 0) {
                Node nUser = graphDb.createNode(NodeType.USER);
                nUser.setProperty(UserProxy.PROP_IDENTIFIER, userIdentifier);
                return nUser;
            }
        } catch (NumberFormatException e) {
            // exception thrown below
        }
        //TODO log exception reason (NaN/<=0)
        throw new IllegalUserIdException(userIdentifier);
    }

    /**
     * Searches the social network graph for an user.
     * 
     * @param userIdentifier
     *            identifier of the user searched
     * @return user node - if the user is existing in social network graph<br>
     *         <b>null</b> - if there is no node representing the user specified
     */
    protected Node findUser(String userIdentifier) {
        try (ResourceIterator<Node> users =
                graphDb.findNodesByLabelAndProperty(NodeType.USER,
                        UserProxy.PROP_IDENTIFIER, userIdentifier).iterator()) {
            if (users.hasNext()) {
                return users.next();
            }
        }
        return null;
    }

    /**
     * Loads a user from social network or lazily creates a new one.
     * 
     * @param userIdentifier
     *            identifier of the user to interact with
     * @return user node - existing or created node representing the user
     * @throws IllegalUserIdException
     *             if the user must be created and the identifier is invalid
     */
    protected Node loadUser(String userIdentifier)
            throws IllegalUserIdException {
        Node nUser = findUser(userIdentifier);
        if (nUser != null) {
            // user is already existing
            return nUser;
        }
        return createUser(userIdentifier);
    }

    @Override
    public boolean addUser(String userIdentifier) throws IllegalUserIdException {
        Node nUser = findUser(userIdentifier);
        if (nUser == null) {
            // user identifier not in use yet
            createUser(userIdentifier);
            return true;
        }
        return false;
    }

    @Override
    public boolean addFollowship(String idFollowing, String idFollowed)
            throws IllegalUserIdException {
        try (Transaction tx = graphDb.beginTx()) {
            if (addFollowship(idFollowing, idFollowed, tx)) {
                tx.success();
                return true;
            }
            return false;
        }
    }

    /**
     * Adds a followship without committing.
     * 
     * @param idFollowing
     * @param idFollowed
     * @param tx
     *            current graph transaction
     * @return
     * @throws IllegalUserIdException
     */
    public boolean addFollowship(
            String idFollowing,
            String idFollowed,
            Transaction tx) throws IllegalUserIdException {
        Node nFollowing = loadUser(idFollowing);
        Node nFollowed = loadUser(idFollowed);

        if (Long.valueOf(idFollowing) < Long.valueOf(idFollowed)) {
            tx.acquireWriteLock(nFollowing);
            tx.acquireWriteLock(nFollowed);
        } else {
            tx.acquireWriteLock(nFollowed);
            tx.acquireWriteLock(nFollowing);
        }

        boolean result = addFollowship(nFollowing, nFollowed);
        if (result) {
            long msCrr = System.currentTimeMillis();
            addStatusUpdate(nFollowing, new StatusUpdate(idFollowing, msCrr,
                    "now follows " + idFollowed), tx);
            addStatusUpdate(nFollowed, new StatusUpdate(idFollowed, msCrr,
                    "has new follower " + idFollowing), tx);
        }
        return result;
    }

    /**
     * Adds a followship between two user nodes to the social network graph.
     * 
     * @param nFollowing
     *            node of the user that wants to follow another user
     * @param nFollowed
     *            node of the user that will be followed
     * @return true - if the followship was successfully created<br>
     *         false - if this followship is already existing
     */
    abstract protected boolean addFollowship(Node nFollowing, Node nFollowed);

    @Override
    public boolean removeFollowship(String idFollowing, String idFollowed)
            throws UnknownFollowingIdException, UnknownFollowedIdException {
        try (Transaction tx = graphDb.beginTx()) {
            if (removeFollowship(idFollowing, idFollowed, tx)) {
                tx.success();
                return true;
            }
            return false;
        }
    }

    /**
     * Removes a followship without committing.
     * 
     * @param idFollowing
     * @param idFollowed
     * @param tx
     *            current graph transaction
     * @return
     * @throws UnknownFollowingIdException
     * @throws UnknownFollowedIdException
     */
    public boolean removeFollowship(
            String idFollowing,
            String idFollowed,
            Transaction tx) throws UnknownFollowingIdException,
            UnknownFollowedIdException {
        Node nFollowing = findUser(idFollowing);
        if (nFollowing == null) {
            throw new UnknownFollowingIdException(idFollowing);
        }
        Node nFollowed = findUser(idFollowed);
        if (nFollowed == null) {
            throw new UnknownFollowedIdException(idFollowed);
        }

        if (Long.valueOf(idFollowing) < Long.valueOf(idFollowed)) {
            tx.acquireWriteLock(nFollowing);
            tx.acquireWriteLock(nFollowed);
        } else {
            tx.acquireWriteLock(nFollowed);
            tx.acquireWriteLock(nFollowing);
        }

        boolean result = removeFollowship(nFollowing, nFollowed);
        if (result) {
            long msCrr = System.currentTimeMillis();
            addStatusUpdate(nFollowing, new StatusUpdate(idFollowing, msCrr,
                    "did unfollow " + idFollowed), tx);
            addStatusUpdate(nFollowed, new StatusUpdate(idFollowed, msCrr,
                    "was unfollowed by " + idFollowing), tx);
        }
        return result;
    }

    /**
     * Removes a followship between two user nodes from the social network
     * graph.
     * 
     * @param nFollowing
     *            node of the user that wants to unfollow a user
     * @param nFollowed
     *            node of the user that will be unfollowed
     * @return true - if the followship was successfully removed<br>
     *         false - if this followship is not existing
     */
    abstract protected boolean
        removeFollowship(Node nFollowing, Node nFollowed);

    @Override
    public long addStatusUpdate(String idAuthor, String message)
            throws IllegalUserIdException {
        try (Transaction tx = graphDb.beginTx()) {
            long statusUpdateId = addStatusUpdate(idAuthor, message, tx);
            if (statusUpdateId != 0) {
                tx.success();
            }
            return statusUpdateId;
        }
    }

    /**
     * Adds a status update without committing.
     * 
     * @param idAuthor
     * @param message
     * @param tx
     *            current graph transaction
     * @return
     * @throws IllegalUserIdException
     */
    public long
        addStatusUpdate(String idAuthor, String message, Transaction tx)
                throws IllegalUserIdException {
        Node nAuthor = loadUser(idAuthor);
        StatusUpdate statusUpdate =
                new StatusUpdate(idAuthor, System.currentTimeMillis(), message);
        return addStatusUpdate(nAuthor, statusUpdate, tx);
    }

    protected long addStatusUpdate(
            Node nAuthor,
            StatusUpdate statusUpdate,
            Transaction tx) {
        tx.acquireWriteLock(nAuthor);
        return addStatusUpdate(nAuthor, statusUpdate);
    }

    /**
     * Adds a status update node to the social network.
     * 
     * @param nAuthor
     *            user node of the status update author
     * @param statusUpdate
     *            status update data
     * @return identifier of the status update node
     */
    abstract protected long addStatusUpdate(
            Node nAuthor,
            StatusUpdate statusUpdate);

    @Override
    public StatusUpdateList readStatusUpdates(
            String idReader,
            int numStatusUpdates) throws UnknownReaderIdException {
        try (Transaction tx = graphDb.beginTx()) {
            StatusUpdateList statusUpdates =
                    readStatusUpdates(idReader, numStatusUpdates, tx);
            return statusUpdates;
        }
    }

    /**
     * Reads a news feed without nested transactions.
     * 
     * @param idReader
     * @param numStatusUpdates
     * @param tx
     *            current graph transaction
     * @return
     * @throws UnknownReaderIdException
     */
    public StatusUpdateList readStatusUpdates(
            String idReader,
            int numStatusUpdates,
            Transaction tx) throws UnknownReaderIdException {
        Node nReader = findUser(idReader);
        if (nReader != null) {
            return readStatusUpdates(nReader, numStatusUpdates);
        }
        throw new UnknownReaderIdException(idReader);
    }

    abstract protected StatusUpdateList readStatusUpdates(
            Node nReader,
            int numStatusUpdates);
}
