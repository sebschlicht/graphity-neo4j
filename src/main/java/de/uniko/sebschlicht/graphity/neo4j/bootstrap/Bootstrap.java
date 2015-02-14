package de.uniko.sebschlicht.graphity.neo4j.bootstrap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.lang.NotImplementedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import de.uniko.sebschlicht.graphity.exception.IllegalUserIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownReaderIdException;
import de.uniko.sebschlicht.graphity.neo4j.EdgeType;
import de.uniko.sebschlicht.graphity.neo4j.Neo4jGraphity;
import de.uniko.sebschlicht.graphity.neo4j.NodeType;
import de.uniko.sebschlicht.graphity.neo4j.impl.WriteOptimizedGraphity;
import de.uniko.sebschlicht.graphity.neo4j.model.StatusUpdateProxy;
import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;
import de.uniko.sebschlicht.socialnet.Subscription;
import de.uniko.sebschlicht.socialnet.requests.Request;
import de.uniko.sebschlicht.socialnet.requests.RequestFollow;
import de.uniko.sebschlicht.socialnet.requests.RequestPost;
import de.uniko.sebschlicht.socialnet.requests.RequestUnfollow;

/**
 * I think this is buggy. Use with caution.
 * 
 * @author sebschlicht
 * 
 */
public class Bootstrap {

    private static boolean _isGraphity = false;

    protected static final Random RANDOM = new Random();

    protected static final char[] POST_SYMBOLS;
    static {
        StringBuilder postSymbols = new StringBuilder();
        // numbers
        for (char number = '0'; number <= '9'; ++number) {
            postSymbols.append(number);
        }
        // lower case letters
        for (char letter = 'a'; letter <= 'z'; ++letter) {
            postSymbols.append(letter);
        }
        // upper case letters
        for (char letter = 'a'; letter <= 'z'; ++letter) {
            postSymbols.append(Character.toUpperCase(letter));
        }
        POST_SYMBOLS = postSymbols.toString().toCharArray();
    }

    private static ArrayList<BootstrapUser> USERS;

    private BatchInserter _inserter;

    private UserManager _users;

    public Bootstrap() {
        _users = new UserManager();
    }

    public Bootstrap(
            String path) {
        _inserter = BatchInserters.inserter(path);
        _users = new UserManager();
    }

    public void loadFromFile(String filePath) throws IOException {
        BootstrapFileLoader fileLoader = new BootstrapFileLoader(filePath);
        Queue<Request> requests = fileLoader.loadRequests();
        mergeRequests(requests);
        System.out.println("bootstrap file loaded.");
    }

    public void bootstrap() {
        // create users
        int numUsers = 0;
        Map<String, Object> userProperties;
        long nodeId;
        for (BootstrapUser user : USERS) {
            if (user == null) {// gaps are no problem
                continue;
            }
            numUsers += 1;
            userProperties = new HashMap<>();
            userProperties.put(UserProxy.PROP_IDENTIFIER,
                    String.valueOf(user.getId()));
            nodeId = _inserter.createNode(userProperties, NodeType.USER);
            user.setNodeId(nodeId);
        }
        System.out.println(numUsers + " users created.");

        // subscribe users
        int numSubscriptions = 0;
        BootstrapUser followed;
        for (BootstrapUser user : USERS) {
            if (user == null) {// gaps are no problem
                continue;
            }
            ArrayList<Long> subscriptions = user.getSubscriptions();
            if (subscriptions == null) {//check if any subscriptions
                continue;
            }
            if (!_isGraphity) {// WriteOptimizedGraphity
                for (long idFollowed : subscriptions) {
                    followed = getUserById(idFollowed);
                    _inserter.createRelationship(user.getNodeId(),
                            followed.getNodeId(), EdgeType.FOLLOWS, null);
                    numSubscriptions += 1;
                }
            } else {// ReadOptimizedGraphity
                throw new NotImplementedException("can not subscribe");
            }
        }
        System.out.println(numSubscriptions + " subscriptions registered.");

        // create posts
        int numUserPosts;
        long numTotalPosts = 0;
        Map<String, Object> postProperties;
        long tsLastPost = System.currentTimeMillis();
        for (BootstrapUser user : USERS) {
            if (user == null) {// gaps are no problem
                continue;
            }
            numUserPosts = user.getNumStatusUpdates();
            if (numUserPosts == 0) {// check if any posts
                continue;
            }
            long[] userPostNodeIds = new long[numUserPosts];
            for (int i = 0; i < numUserPosts; ++i) {
                postProperties = new HashMap<>();
                postProperties
                        .put(StatusUpdateProxy.PROP_PUBLISHED, tsLastPost);
                postProperties.put(StatusUpdateProxy.PROP_MESSAGE,
                        generatePostMessage(140));
                nodeId = _inserter.createNode(postProperties, NodeType.UPDATE);
                userPostNodeIds[i] = nodeId;
                tsLastPost += 1;
            }
            user.setPostNodeIds(userPostNodeIds);
            numTotalPosts += numUserPosts;
        }
        System.out.println(numTotalPosts + " posts created.");

        // link posts
        for (BootstrapUser user : USERS) {
            if (user == null) {// gaps are no problem
                continue;
            }
            long[] postNodeIds = user.getPostNodeIds();
            if (postNodeIds == null) {// check if any posts
                continue;
            }
            for (int iPost = 0; iPost < postNodeIds.length; ++iPost) {
                if (iPost + 1 < postNodeIds.length) {// newerPost -> olderPost
                    _inserter.createRelationship(postNodeIds[iPost + 1],
                            postNodeIds[iPost], EdgeType.PUBLISHED, null);
                } else {// user -> newestPost
                    _inserter.createRelationship(user.getNodeId(),
                            postNodeIds[iPost], EdgeType.PUBLISHED, null);
                }
            }
        }
        System.out.println("posts linked.");
    }

    private void mergeRequests(Queue<Request> requests) {
        System.out.println("merging requests...");
        TreeSet<Subscription> subscriptions = new TreeSet<Subscription>();

        /** we know that user id is in Integer range */
        // expand user array
        long highestId = -1;
        long userId = -1;
        for (Request request : requests) {
            switch (request.getType()) {
                case FOLLOW:
                    userId = ((RequestFollow) request).getIdSubscriber();
                    break;

                case POST:
                    userId = ((RequestPost) request).getId();
                    break;

                case UNFOLLOW:
                    userId = ((RequestUnfollow) request).getIdSubscriber();
                    break;
            }
            if (userId > highestId) {
                highestId = userId;
            }
        }
        int capacity = (int) (highestId / 0.75) + 1;
        USERS = new ArrayList<BootstrapUser>(capacity);
        for (int i = USERS.size(); i < highestId; ++i) {// OMFG what a bug in usability
            USERS.add(null);
        }
        // load current subscription state
        BootstrapUser user;
        for (int i = 0; i < USERS.size(); ++i) {
            userId = i + 1;
            user = USERS.get(i);
            if (user != null && user.getSubscriptions() != null) {
                for (long idFollowed : user.getSubscriptions()) {
                    subscriptions.add(new Subscription(userId, idFollowed));
                }
            }

        }

        RequestFollow rfo;
        RequestPost rp;
        RequestUnfollow ru;

        Subscription subscription;
        for (Request request : requests) {
            switch (request.getType()) {
                case FOLLOW:
                    rfo = (RequestFollow) request;
                    subscription =
                            new Subscription(rfo.getIdSubscriber(),
                                    rfo.getIdFollowed());
                    subscriptions.add(subscription);
                    user = loadUserById(rfo.getIdSubscriber());
                    user.addStatusUpdate();
                    user = loadUserById(rfo.getIdFollowed());
                    user.addStatusUpdate();
                    break;

                case POST:
                    rp = (RequestPost) request;
                    user = loadUserById(rp.getId());
                    user.addStatusUpdate();
                    break;

                case UNFOLLOW:
                    ru = (RequestUnfollow) request;
                    subscription =
                            new Subscription(ru.getIdSubscriber(),
                                    ru.getIdFollowed());
                    subscriptions.remove(subscription);
                    user = loadUserById(ru.getIdSubscriber());
                    user.addStatusUpdate();
                    user = loadUserById(ru.getIdFollowed());
                    user.addStatusUpdate();
            }
        }
        // set new subscription state
        long idPrevious = 0;
        for (Subscription sub : subscriptions) {
            user = getUserById(sub.getIdSubscriber());
            if (user == null) {
                user = new BootstrapUser(sub.getIdSubscriber());
                USERS.set((int) sub.getIdSubscriber() - 1, user);
            }
            if (idPrevious != sub.getIdSubscriber()
                    && user.getSubscriptions() != null) {
                user.getSubscriptions().clear();
            }
            user.addSubscription(sub.getIdFollowed());
            idPrevious = sub.getIdSubscriber();
        }
    }

    private static BootstrapUser getUserById(long id) {
        return USERS.get((int) id - 1);
    }

    private static BootstrapUser loadUserById(long id) {
        BootstrapUser user = getUserById(id);
        if (user == null) {
            user = new BootstrapUser(id);
            USERS.set((int) id - 1, user);
        }
        return user;
    }

    private static int getFeedSize(long idReader) {
        BootstrapUser reader = getUserById(idReader);
        int feedSize = 0;
        BootstrapUser followed;
        if (reader.getSubscriptions() != null) {
            for (long idFollowed : reader.getSubscriptions()) {
                followed = getUserById(idFollowed);
                if (followed != null) {
                    feedSize += followed.getNumStatusUpdates();
                }
            }
        }
        return feedSize;
    }

    public void
        bootstrap(long[] userIds, String[] subscriptions, int[] numPosts) {
        // load users
        _users.setUserIds(userIds);
        for (int i = 0; i < userIds.length; ++i) {
            _users.addUserByIndex(i);
        }
        System.out.println("users loaded.");
        createUsers(userIds);
        System.out.println(userIds.length + " users created.");

        // subscribe users
        long numSubscriptions = createSubscriptions(subscriptions);
        System.out.println(numSubscriptions + " subscriptions registered.");

        // load, create and link posts
        long numPostsTotal = loadPosts(numPosts);
        System.out.println(numPostsTotal + " posts loaded.");
        createPosts(numPosts);
        System.out.println("posts created.");
        linkPosts(numPosts);
        System.out.println("posts linked.");
    }

    private void createUsers(long[] userIds) {
        User user;
        Map<String, Object> userProperties;
        long nodeId;
        for (long id : userIds) {
            user = _users.getUser(id);
            userProperties = new HashMap<>();
            userProperties.put(UserProxy.PROP_IDENTIFIER, String.valueOf(id));
            nodeId = _inserter.createNode(userProperties, NodeType.USER);
            user.setNodeId(nodeId);
        }
    }

    private long loadPosts(int[] numPosts) {
        User user;
        int numUserPosts;
        long numTotalPosts = 0;
        for (int i = 0; i < numPosts.length; ++i) {
            user = _users.getUserByIndex(i);
            numUserPosts = numPosts[i];
            // check if any posts
            if (numUserPosts == 0) {
                continue;
            }
            user.setPostNodeIds(new long[numUserPosts]);
            numTotalPosts += numUserPosts;
        }
        return numTotalPosts;
    }

    private void createPosts(int[] numPosts) {
        User user;
        int numUserPosts;
        Map<String, Object> postProperties;
        long tsLastPost = System.currentTimeMillis();
        long nodeId;
        for (int i = 0; i < numPosts.length; ++i) {
            user = _users.getUserByIndex(i);
            numUserPosts = numPosts[i];
            // check if any posts
            if (numUserPosts == 0) {
                continue;
            }
            long[] userPostNodes = user.getPostNodeIds();
            for (int iPost = 0; iPost < numUserPosts; ++iPost) {
                postProperties = new HashMap<>();
                postProperties
                        .put(StatusUpdateProxy.PROP_PUBLISHED, tsLastPost);
                postProperties.put(StatusUpdateProxy.PROP_MESSAGE,
                        generatePostMessage(140));
                nodeId = _inserter.createNode(postProperties, NodeType.UPDATE);
                userPostNodes[iPost] = nodeId;
                tsLastPost += 1;
            }
            // update last_post
            _inserter.setNodeProperty(user.getNodeId(),
                    UserProxy.PROP_LAST_STREAM_UDPATE, tsLastPost);
        }
    }

    private void linkPosts(int[] numPosts) {
        User user;
        for (int i = 0; i < numPosts.length; ++i) {
            user = _users.getUserByIndex(i);
            if (user.getPostNodeIds() == null) {
                continue;
            }
            long[] postNodeIds = user.getPostNodeIds();
            for (int iPost = 0; iPost < postNodeIds.length; ++iPost) {
                if (iPost + 1 < postNodeIds.length) {// newerPost -> olderPost
                    _inserter.createRelationship(postNodeIds[iPost + 1],
                            postNodeIds[iPost], EdgeType.PUBLISHED, null);
                } else {// user -> newestPost
                    _inserter.createRelationship(user.getNodeId(),
                            postNodeIds[iPost], EdgeType.PUBLISHED, null);
                }
            }
        }
    }

    private long createSubscriptions(String[] subscriptions) {
        User user, followed;
        long idFollowed;
        long numSubscriptions = 0;
        for (int i = 0; i < subscriptions.length; ++i) {
            user = _users.getUserByIndex(i);
            String sUserSubscriptions = subscriptions[i];
            // check if any subscriptions
            if ("".equals(sUserSubscriptions)) {
                continue;
            }
            String[] aUserSubscriptions = sUserSubscriptions.split(",");
            if (!_isGraphity) {// WriteOptimizedGraphity
                for (int iFollowed = 0; iFollowed < aUserSubscriptions.length; ++iFollowed) {
                    idFollowed = Long.valueOf(aUserSubscriptions[iFollowed]);
                    followed = _users.getUser(idFollowed);
                    _inserter.createRelationship(user.getNodeId(),
                            followed.getNodeId(), EdgeType.FOLLOWS, null);
                    numSubscriptions += 1;
                }
            } else {// ReadOptimizedGraphity
                throw new NotImplementedException("can not subscribe");
            }
        }
        return numSubscriptions;
    }

    public void shutdown() {
        _inserter.shutdown();
    }

    protected static String generatePostMessage(int length) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; ++i) {
            builder.append(getRandomPostChar());
        }
        return builder.toString();
    }

    protected static char getRandomPostChar() {
        return POST_SYMBOLS[RANDOM.nextInt(POST_SYMBOLS.length)];
    }

    protected static boolean deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (null != files) {
                for (File child : files) {
                    if (child.isDirectory()) {
                        deleteDirectory(child);
                    } else {
                        child.delete();
                    }
                }
            }
        }
        return directory.delete();
    }

    public static void main(String[] args) throws IOException,
            IllegalUserIdException, UnknownReaderIdException {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "arguments: 1. database path, 2. bootstrap file path");
        }
        boolean debug = true;
        File fDatabase = new File(args[0]);
        if (!debug) {
            if (fDatabase.exists()) {
                deleteDirectory(fDatabase);
            }
            System.out.println("initializing database...");
            final Bootstrap bootstrapper =
                    new Bootstrap(fDatabase.getAbsolutePath());
            Runtime.getRuntime().addShutdownHook(new Thread() {

                @Override
                public void run() {
                    System.out
                            .println("process finished. making persistent...");
                    bootstrapper.shutdown();
                    System.out.println("exited.");
                }
            });
            System.out.println("database ready.");
            bootstrapper.loadFromFile(args[1]);
            bootstrapper.bootstrap();
            System.out.println("process finished. making persistent...");
            bootstrapper.shutdown();
            System.out.println("exited.");
        }

        System.out.println("initializing database...");
        Bootstrap bootstrapper = new Bootstrap();
        final GraphDatabaseService graphDb =
                new GraphDatabaseFactory().newEmbeddedDatabase(fDatabase
                        .getAbsolutePath());
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                graphDb.shutdown();
                System.out.println("database shut down.");
            }
        });
        System.out.println("database ready.");
        bootstrapper.loadFromFile(args[1]);
        Neo4jGraphity graphity = new WriteOptimizedGraphity(graphDb);
        graphity.init();

        int feedSize;
        int numUsersChecked = 0;
        int numSure = 0;
        for (BootstrapUser user : USERS) {
            if (user == null) {
                continue;
            }
            feedSize = getFeedSize(user.getId());
            if (graphity.readStatusUpdates(String.valueOf(user.getId()),
                    Integer.MAX_VALUE).size() != feedSize) {
                System.err.println("feed size mismatch: expected "
                        + feedSize
                        + " but was "
                        + graphity
                                .readStatusUpdates(
                                        String.valueOf(user.getId()),
                                        Integer.MAX_VALUE).size()
                        + " for user " + user.getId());
                return;
            }
            numUsersChecked += 1;
            if (feedSize > 0) {
                numSure += 1;
            }
            if (numUsersChecked % 50000 == 0) {
                System.out.println(numUsersChecked + " users checked ("
                        + numSure + ")");
            }
        }
        System.out.println("OMFG WOOOOOOW!");
    }
}
