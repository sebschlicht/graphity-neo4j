package de.uniko.sebschlicht.graphity.neo4j.bootstrap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import de.uniko.sebschlicht.graphity.bootstrap.BootstrapClient;
import de.uniko.sebschlicht.graphity.bootstrap.User;
import de.uniko.sebschlicht.graphity.neo4j.EdgeType;
import de.uniko.sebschlicht.graphity.neo4j.NodeType;
import de.uniko.sebschlicht.graphity.neo4j.impl.ReadOptimizedGraphity;
import de.uniko.sebschlicht.graphity.neo4j.model.StatusUpdateProxy;
import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;

public class Neo4jBootstrapper extends BootstrapClient {

    private BatchInserter _inserter;

    public Neo4jBootstrapper(
            String databasePath,
            boolean isGraphity) {
        super(isGraphity);
        _inserter = BatchInserters.inserter(databasePath);
    }

    public void shutdown() {
        _inserter.shutdown();
    }

    @Override
    protected long createUsers() {
        long numUsers = 0, nodeId;
        Map<String, Object> userProperties;
        ArrayList<User> tmp = new ArrayList<>();
        Label[] nolabel = new Label[0];
        for (User user : _users.getUsers()) {
            userProperties = new HashMap<>();
            userProperties.put(UserProxy.PROP_IDENTIFIER,
                    String.valueOf(user.getId()));
            userProperties.put(UserProxy.PROP_LAST_STREAM_UDPATE,
                    user.getTsLastPost());
            nodeId = _inserter.createNode(userProperties, NodeType.USER);
            user.setNodeId(nodeId);
            numUsers += 1;

            if (_isGraphity) {
                long[] subscriptions = user.getSubscriptions();
                if (subscriptions == null) {// can this happen?
                    continue;
                }
                long[] replicas = new long[subscriptions.length];

                // sort subscriptions and create replica nodes
                for (long idFollowed : subscriptions) {
                    tmp.add(_users.getUser(idFollowed));
                }
                Collections.sort(tmp);
                int i = 0;
                for (User followed : tmp) {
                    subscriptions[i] = followed.getId();
                    nodeId = _inserter.createNode(null, nolabel);
                    replicas[i] = nodeId;
                    i += 1;
                }
                user.setReplicas(replicas);
                tmp.clear();
            }
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
            if (!_isGraphity) {// WriteOptimizedGraphity
                for (long idFollowed : subscriptions) {
                    User followed = _users.getUser(idFollowed);
                    _inserter.createRelationship(user.getNodeId(),
                            followed.getNodeId(), EdgeType.FOLLOWS, null);
                    numSubscriptions += 1;
                }
            } else {// ReadOptimizedGraphity
                // link users and replica layer
                long prev = user.getNodeId();
                RelationshipType graphity =
                        ReadOptimizedGraphity.graphityIndexType(user.getId());
                for (long subscription : subscriptions) {
                    User followed = _users.getUser(subscription);
                    // user -> FOLLOWS -> user
                    _inserter.createRelationship(user.getNodeId(),
                            followed.getNodeId(), EdgeType.FOLLOWS, null);
                    // subscriber/user -> GRAPHITY -> user
                    _inserter.createRelationship(prev, followed.getNodeId(),
                            graphity, null);
                    prev = followed.getNodeId();
                    numSubscriptions += 1;
                }
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
                if (iPost == userPostNodes.length - 1) {
                    user.setTsLastPost(tsLastPost);
                }
                tsLastPost += 1;
                numTotalPosts += 1;
            }
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

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out
                    .println("usage: Neo4jBootstrapper <pathBootstrapLog> <pathNeo4jDb> <algorithm {stou|graphity}>");
            throw new IllegalArgumentException("invalid number of arguments");
        }
        File fBootstrapLog = new File(args[0]);
        File fDatabase = new File(args[1]);
        boolean isGraphity;
        String sAlgorithm = args[2];
        if ("stou".equalsIgnoreCase(sAlgorithm)) {
            isGraphity = false;
            System.out.println("STOU model set");
        } else if ("graphity".equalsIgnoreCase(sAlgorithm)) {
            isGraphity = true;
            System.out.println("Graphity model set");
        } else {
            throw new IllegalArgumentException(
                    "Invalid social network algorithm! Use \"stou\" or \"graphity\".");
        }
        final Neo4jBootstrapper bootstrapClient =
                new Neo4jBootstrapper(fDatabase.getAbsolutePath(), isGraphity);

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
