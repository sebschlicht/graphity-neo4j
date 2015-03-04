package de.uniko.sebschlicht.graphity.neo4j.bootstrap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import de.uniko.sebschlicht.graphity.bootstrap.BootstrapClient;
import de.uniko.sebschlicht.graphity.bootstrap.User;
import de.uniko.sebschlicht.graphity.bootstrap.UserManager;
import de.uniko.sebschlicht.graphity.bootstrap.generate.MutableState;
import de.uniko.sebschlicht.graphity.bootstrap.load.BootstrapLoader;
import de.uniko.sebschlicht.graphity.neo4j.EdgeType;
import de.uniko.sebschlicht.graphity.neo4j.NodeType;
import de.uniko.sebschlicht.graphity.neo4j.model.StatusUpdateProxy;
import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;
import de.uniko.sebschlicht.socialnet.Subscription;

public class Neo4jBootstrapClient extends BootstrapClient {

    private BatchInserter _inserter;

    public Neo4jBootstrapClient(
            String databasePath) {
        _inserter = BatchInserters.inserter(databasePath);
    }

    public void shutdown() {
        _inserter.shutdown();
    }

    /**
     * Bulk loads a social network state into the news feed service storage.
     * The social network state is handled by the Neo4j server plugin REST API.
     * This method has to load all the data in a single call.
     * 
     * @param userIds
     *            identifiers of all users existing
     * @param subscriptions
     *            array with following content schema:<br>
     *            numSubscriptions, idFollowed_0 .. idFollowed_numSubscriptions<br>
     *            per user, where users are sorted as in userIds
     * @param numPosts
     *            number of posts per user, where users are sorted as in userIds
     */
    public void bootstrap(long[] userIds, long[] subscriptions, int[] numPosts) {
        long numUsers = createUsers(userIds);
        System.out.println(numUsers + " users created.");

        // subscribe users
        long numSubscriptions = createSubscriptions(subscriptions);
        System.out.println(numSubscriptions + " subscriptions registered.");

        // create and link posts
        long numPostsTotal = createPosts(numPosts);
        System.out.println(numPostsTotal + " posts created.");
        numPostsTotal = linkPosts(numPosts);
        System.out.println(numPostsTotal + " posts linked.");
    }

    /**
     * Adds the users to the database.
     * 
     * @param userIds
     *            identifiers of all users existing
     */
    protected long createUsers(long[] userIds) {
        long numUsers = 0;
        User user;
        Map<String, Object> userProperties;
        long nodeId;
        _users.setUserIds(userIds);
        for (long id : userIds) {
            user = _users.addUser(id);
            userProperties = new HashMap<>();
            userProperties.put(UserProxy.PROP_IDENTIFIER, String.valueOf(id));
            nodeId = _inserter.createNode(userProperties, NodeType.USER);
            user.setNodeId(nodeId);
            numUsers += 1;
        }
        return numUsers;
    }

    /**
     * Links the users in the database.
     * 
     * @param subscriptions
     *            array with following content schema:<br>
     *            numSubscriptions, idFollowed_0 .. idFollowed_numSubscriptions<br>
     *            per user, where users are sorted as in userIds
     * @return number of subscriptions
     */
    protected long createSubscriptions(long[] subscriptions) {
        User user, followed;
        long numSubscriptions = 0;
        int iUser = 0;
        long numUserSubscriptions, idFollowed;
        for (int iSubscription = 0; iSubscription < subscriptions.length; ++iUser) {// iSubscription is incremented in the loop
            numUserSubscriptions = subscriptions[iSubscription++];
            if (numUserSubscriptions == 0) {
                continue;
            }
            user = _users.getUserByIndex(iUser);
            if (!IS_GRAPHITY) {// WriteOptimizedGraphity
                for (int i = 0; i < numUserSubscriptions; ++i) {
                    idFollowed = subscriptions[iSubscription++];
                    followed = _users.getUser(idFollowed);
                    _inserter.createRelationship(user.getNodeId(),
                            followed.getNodeId(), EdgeType.FOLLOWS, null);
                }
                numSubscriptions += numUserSubscriptions;
            } else {// ReadOptimizedGraphity
                throw new IllegalStateException("can not subscribe");
            }
        }
        return numSubscriptions;
    }

    /**
     * Adds the posts to the database.
     * 
     * @param numPosts
     *            number of posts per user, where users are sorted as in userIds
     * @return number of posts created
     */
    protected long createPosts(int[] numPosts) {
        long numTotalPosts = 0;
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
            long[] userPostNodes = new long[numUserPosts];
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
            user.setPostNodeIds(userPostNodes);
            numTotalPosts += numUserPosts;
            // update last_post
            _inserter.setNodeProperty(user.getNodeId(),
                    UserProxy.PROP_LAST_STREAM_UDPATE, tsLastPost);
        }
        return numTotalPosts;
    }

    /**
     * Links the posts in the database.
     * 
     * @param numPosts
     *            number of posts per user, where users are sorted as in userIds
     * @return number of posts linked
     */
    protected long linkPosts(int[] numPosts) {
        long numTotalPosts = 0;
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
            numTotalPosts += postNodeIds.length;
        }
        return numTotalPosts;
    }

    @Override
    protected long createUsers() {
        long numUsers = 0, nodeId;
        Map<String, Object> userProperties;
        for (User user : _users.getUsers()) {
            userProperties = new HashMap<>();
            userProperties.put(UserProxy.PROP_IDENTIFIER,
                    String.valueOf(user.getId()));
            nodeId = _inserter.createNode(userProperties, NodeType.USER);
            user.setNodeId(nodeId);
            numUsers += 1;
        }
        return numUsers;
    }

    @Override
    protected long createSubscriptions() {
        long numSubscriptions = 0;
        for (User user : _users.getUsers()) {
            long[] subscriptions = user.getSubscriptions();
            if (subscriptions == null) {// can this happen?
                continue;
            }
            if (!IS_GRAPHITY) {// WriteOptimizedGraphity
                for (long idFollowed : subscriptions) {
                    User followed = _users.getUser(idFollowed);
                    _inserter.createRelationship(user.getNodeId(),
                            followed.getNodeId(), EdgeType.FOLLOWS, null);
                    numSubscriptions += 1;
                }
            } else {// ReadOptimizedGraphity
                throw new IllegalStateException("can not subscribe");
            }
        }
        return numSubscriptions;
    }

    @Override
    protected long createPosts() {
        long numTotalPosts = 0;
        Map<String, Object> postProperties;
        long tsLastPost = System.currentTimeMillis();
        long nodeId;
        for (User user : _users.getUsers()) {
            long[] userPostNodes = user.getPostNodeIds();
            for (int iPost = 0; iPost < userPostNodes.length; ++iPost) {
                postProperties = new HashMap<>();
                postProperties
                        .put(StatusUpdateProxy.PROP_PUBLISHED, tsLastPost);
                postProperties.put(StatusUpdateProxy.PROP_MESSAGE,
                        generatePostMessage(140));
                nodeId = _inserter.createNode(postProperties, NodeType.UPDATE);
                userPostNodes[iPost] = nodeId;
                tsLastPost += 1;
                numTotalPosts += 1;
            }
            // update last_post
            _inserter.setNodeProperty(user.getNodeId(),
                    UserProxy.PROP_LAST_STREAM_UDPATE, tsLastPost);
        }
        return numTotalPosts;
    }

    @Override
    protected long linkPosts() {
        long numTotalPosts = 0;
        for (User user : _users.getUsers()) {
            long[] postNodeIds = user.getPostNodeIds();
            if (postNodeIds == null) {// should not happen
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
            numTotalPosts += postNodeIds.length;
        }
        return numTotalPosts;
    }

    private void neo4jBootstrap(File fBootstrapLog) throws IOException {
        BootstrapLoader bootstrapLoader =
                new BootstrapLoader(fBootstrapLog, true);
        MutableState state = bootstrapLoader.getState();
        _users = new UserManager();

        TreeSet<Long> sUserIds = new TreeSet<>();
        ArrayList<Long> aUserIds = new ArrayList<>();

        // convert from mutable to final social network state
        // Note: This will happen on the client machine!
        long prevUserId = -1, userId = 0;
        ArrayList<Long> aSubscriptions = new ArrayList<>();
        List<Long> userSubs = new LinkedList<>();

        // load subscriptions and followers
        TreeSet<Subscription> subscriptions = state.getSubscriptions();
        int iSubscription = 0;
        int numSubscriptions = subscriptions.size();
        for (Subscription subscription : subscriptions) {
            // switch to current user
            userId = subscription.getIdSubscriber();
            if (userId != prevUserId) {
                // make previous user persistent when switching to a new user
                aSubscriptions.add((long) userSubs.size());
                for (long userSub : userSubs) {
                    aSubscriptions.add(userSub);
                }
                // switch to new user
                sUserIds.add(userId);
                aUserIds.add(userId);
                userSubs.clear();
                prevUserId = userId;
            }

            // add subscription for current user
            userSubs.add(subscription.getIdFollowed());

            // make persistent if last user
            if (iSubscription == numSubscriptions - 1) {
                aSubscriptions.add((long) userSubs.size());
                for (long userSub : userSubs) {
                    aSubscriptions.add(userSub);
                }
            }
            iSubscription += 1;
        }

        // load posts and authors
        ArrayList<Integer> aNumPosts = new ArrayList<Integer>();
        Map<Long, int[]> numPosts = state.getNumPosts();
        for (long crrUserId : aUserIds) {// load posts from users with active subscriptions
            int[] numUserPosts = numPosts.get(crrUserId);
            aNumPosts.add(numUserPosts[1]);// total number of posts // we are bootstrapping!
        }
        for (Map.Entry<Long, int[]> entry : numPosts.entrySet()) {
            userId = entry.getKey();
            if (sUserIds.contains(userId)) {// user with active subscriptions
                continue;
            }
            int[] numUserPosts = entry.getValue();
            aUserIds.add(userId);
            aNumPosts.add(numUserPosts[1]);// total number of posts // we are bootstrapping!
        }

        long[] userIds = listToArray(aUserIds);
        long[] arraySubscriptions = listToArray(aSubscriptions);
        int[] arrayNumPosts = new int[aNumPosts.size()];
        int i = 0;
        for (int value : aNumPosts) {
            arrayNumPosts[i] = value;
            i += 1;
        }

        bootstrap(userIds, arraySubscriptions, arrayNumPosts);
    }

    public static void main(String[] args) throws IOException {
        File fDatabase = new File("/media/shared/neobootfile");
        File fBootstrapLog = new File("bootstrap.log");
        final Neo4jBootstrapClient bootstrapClient =
                new Neo4jBootstrapClient(fDatabase.getAbsolutePath());

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                System.out.println("process finished. making persistent...");
                bootstrapClient.shutdown();
                System.out.println("exited.");
            }
        });
        System.out.println("database ready.");
        bootstrapClient.bootstrap(fBootstrapLog);
    }
}
