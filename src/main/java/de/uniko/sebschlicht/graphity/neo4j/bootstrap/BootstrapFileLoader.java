package de.uniko.sebschlicht.graphity.neo4j.bootstrap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.lang.NotImplementedException;

import de.uniko.sebschlicht.socialnet.requests.Request;
import de.uniko.sebschlicht.socialnet.requests.RequestFeed;
import de.uniko.sebschlicht.socialnet.requests.RequestFollow;
import de.uniko.sebschlicht.socialnet.requests.RequestPost;
import de.uniko.sebschlicht.socialnet.requests.RequestType;
import de.uniko.sebschlicht.socialnet.requests.RequestUnfollow;

public class BootstrapFileLoader extends CsvParser<Request> {

    public BootstrapFileLoader(
            String filePath) throws FileNotFoundException {
        super(filePath);
    }

    public Queue<Request> loadRequests() throws IOException {
        Queue<Request> requests = new LinkedList<>();

        Request request;
        while ((request = getRequest()) != null) {
            requests.add(request);
        }
        return requests;
    }

    public Request getRequest() throws IOException {
        String[] entry = getEntry();
        if (entry == null) {
            return null;
        }
        if (entry.length == 0) {// skip empty line
            return getRequest();
        }
        int typeId = Integer.valueOf(entry[0]);
        RequestType type = RequestType.getTypeById(typeId);
        if (type == null) {
            throw new NotImplementedException(
                    "unknown request type identifier \"" + typeId + "\"");
        }

        long idActor = Long.valueOf(entry[1]);
        long idFollowed;
        switch (type) {
            case FEED:
                return new RequestFeed(idActor);

            case FOLLOW:
                idFollowed = Long.valueOf(entry[2]);
                return new RequestFollow(idActor, idFollowed);

            case POST:
                return new RequestPost(idActor, entry[2]);

            case UNFOLLOW:
                idFollowed = Long.valueOf(entry[2]);
                return new RequestUnfollow(idActor, idFollowed);

            default:
                throw new NotImplementedException("unknown request type \""
                        + type + "\"");
        }
    }
}
