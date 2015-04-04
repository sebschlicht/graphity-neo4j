package de.uniko.sebschlicht.graphity.neo4j;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;

import de.uniko.sebschlicht.graphity.Graphity;
import de.uniko.sebschlicht.graphity.exception.IllegalUserIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownFollowedIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownFollowingIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownReaderIdException;
import de.uniko.sebschlicht.graphity.neo4j.impl.LockManager;
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

    protected long checkUserId(String userIdentifier)
            throws IllegalUserIdException {
        try {
            long idUser = Long.valueOf(userIdentifier);
            if (idUser > 0) {
                return idUser;
            }
        } catch (NumberFormatException e) {
            // exception thrown below
        }
        //TODO log exception reason (NaN/<=0)
        throw new IllegalUserIdException(userIdentifier);
    }

    /**
     * Creates a user that can act in the social network.
     * 
     * @param userIdentifier
     *            identifier of the new user
     * @return user node
     */
    public Node createUser(long userIdentifier) {
        Node nUser = graphDb.createNode(NodeType.USER);
        nUser.setProperty(UserProxy.PROP_IDENTIFIER, userIdentifier);
        return nUser;
    }

    /**
     * Searches the social network graph for an user.
     * 
     * @param userIdentifier
     *            identifier of the user searched
     * @return user node - if the user is existing in social network graph<br>
     *         <b>null</b> - if there is no node representing the user specified
     */
    protected Node findUserNode(long userIdentifier) {
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
     * Loads a user from the graph.
     * 
     * @param userIdentifier
     *            identifier of the user
     * @return user proxy - if the user is existing<br>
     *         <b>null</b> if there is no user with the identifier specified
     */
    protected UserProxy findUser(long userIdentifier) {
        Node nUser = findUserNode(userIdentifier);
        if (nUser == null) {
            return null;
        }
        UserProxy user = new UserProxy(nUser);
        user.cacheIdentifier(userIdentifier);
        return user;
    }

    /**
     * Loads a user from social network or lazily creates a new one.
     * 
     * @param userIdentifier
     *            identifier of the user to interact with
     * @return user proxy - existing or created user node wrapped in a proxy
     * @throws IllegalUserIdException
     *             if the user identifier is invalid
     */
    protected UserProxy loadUser(long userIdentifier)
            throws IllegalUserIdException {
        UserProxy user = findUser(userIdentifier);
        if (user != null) {
            // user is already existing
            return user;
        }
        user = new UserProxy(createUser(userIdentifier));
        user.cacheIdentifier(userIdentifier);
        return user;
    }

    @Override
    public boolean addUser(String userIdentifier) throws IllegalUserIdException {
        long idUser = checkUserId(userIdentifier);
        Node nUser = findUserNode(idUser);
        if (nUser == null) {
            // user identifier not in use yet
            createUser(idUser);
            return true;
        }
        return false;
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
                addStatusUpdate(followed, new StatusUpdate(sIdFollowed, msCrr,
                        "has new follower " + sIdFollowing), tx);
                tx.success();
                return true;
            }
            return false;
        }
    }

    /**
     * Adds a followship between two user nodes to the social network graph.
     * 
     * @param following
     *            user that wants to follow another user
     * @param followed
     *            user that will be followed
     * @return true - if the followship was successfully created<br>
     *         false - if this followship is already existing
     */
    abstract protected boolean addFollowship(
            UserProxy following,
            UserProxy followed);

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
                addStatusUpdate(following, new StatusUpdate(sIdFollowing,
                        msCrr, "did unfollow " + sIdFollowed), tx);
                addStatusUpdate(followed, new StatusUpdate(sIdFollowed, msCrr,
                        "was unfollowed by " + sIdFollowing), tx);
                tx.success();
                return true;
            }
            return false;
        }
    }

    /**
     * Removes a followship between two users from the social network graph.
     * 
     * @param following
     *            user that wants to unfollow a user
     * @param followed
     *            user that will be unfollowed
     * @return true - if the followship was successfully removed<br>
     *         false - if this followship is not existing
     */
    abstract protected boolean removeFollowship(
            UserProxy following,
            UserProxy followed);

    @Override
    public long addStatusUpdate(String sIdAuthor, String message)
            throws IllegalUserIdException {
        long idAuthor = checkUserId(sIdAuthor);
        try (Transaction tx = graphDb.beginTx()) {
            UserProxy uAuthor = loadUser(idAuthor);
            StatusUpdate statusUpdate =
                    new StatusUpdate(sIdAuthor, System.currentTimeMillis(),
                            message);

            long statusUpdateId = addStatusUpdate(uAuthor, statusUpdate, tx);
            if (statusUpdateId != 0) {
                tx.success();
            }
            return statusUpdateId;
        }
    }

    protected long addStatusUpdate(
            UserProxy uAuthor,
            StatusUpdate statusUpdate,
            Transaction tx) {
        return addStatusUpdate(uAuthor, statusUpdate);
    }

    /**
     * Adds a status update node to the social network.
     * 
     * @param uAuthor
     *            status update author proxy
     * @param statusUpdate
     *            status update data
     * @return identifier of the status update node
     */
    abstract protected long addStatusUpdate(
            UserProxy uAuthor,
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
        Node nReader = findUserNode(idReader);
        if (nReader != null) {
            return readStatusUpdates(nReader, numStatusUpdates);
        }
        throw new UnknownReaderIdException(idReader);
    }

    abstract protected StatusUpdateList readStatusUpdates(
            Node nReader,
            int numStatusUpdates);
}
