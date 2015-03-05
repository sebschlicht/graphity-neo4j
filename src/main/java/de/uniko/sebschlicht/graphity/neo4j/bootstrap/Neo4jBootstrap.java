package de.uniko.sebschlicht.graphity.neo4j.bootstrap;

public interface Neo4jBootstrap {

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
    public void bootstrap(long[] userIds, long[] subscriptions, int[] numPosts);
}
