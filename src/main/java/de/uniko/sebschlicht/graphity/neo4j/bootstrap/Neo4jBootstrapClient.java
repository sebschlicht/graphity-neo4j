package de.uniko.sebschlicht.graphity.neo4j.bootstrap;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import de.uniko.sebschlicht.graphity.bootstrap.BootstrapClient;
import de.uniko.sebschlicht.graphity.bootstrap.User;
import de.uniko.sebschlicht.graphity.neo4j.EdgeType;
import de.uniko.sebschlicht.graphity.neo4j.NodeType;
import de.uniko.sebschlicht.graphity.neo4j.model.StatusUpdateProxy;
import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;

public class Neo4jBootstrapClient extends BootstrapClient {

    private BatchInserter _inserter;

    public Neo4jBootstrapClient(
            String databasePath) {
        _inserter = BatchInserters.inserter(databasePath);
    }

    public void shutdown() {
        _inserter.shutdown();
    }

    @Override
    protected void createUsers(long[] userIds) {
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

    @Override
    protected long createSubscriptions(List<long[]> subscriptions) {
        User user, followed;
        long numSubscriptions = 0;
        int i = 0;
        for (long[] userSubscriptions : subscriptions) {
            if (userSubscriptions == null) {
                continue;
            }
            user = _users.getUserByIndex(i);
            if (!IS_GRAPHITY) {// WriteOptimizedGraphity
                for (long idFollowed : userSubscriptions) {
                    followed = _users.getUser(idFollowed);
                    _inserter.createRelationship(user.getNodeId(),
                            followed.getNodeId(), EdgeType.FOLLOWS, null);
                    numSubscriptions += 1;
                }
            } else {// ReadOptimizedGraphity
                throw new IllegalStateException("can not subscribe");
            }
            i += 1;
        }
        return numSubscriptions;
    }

    @Override
    protected long loadPosts(int[] numPosts) {
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

    @Override
    protected void createPosts(int[] numPosts) {
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

    @Override
    protected void linkPosts(int[] numPosts) {
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

    public static void main(String[] args) throws IOException {
        File fDatabase = new File("/tmp/neo4jbl");
        File fBootstrapLog = new File("/tmp/bootstrap.log");
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
