package de.uniko.sebschlicht.graphity.neo4j.model;

import org.neo4j.graphdb.Node;

import de.metalcon.domain.Muid;
import de.metalcon.domain.UidType;
import de.metalcon.exceptions.ServiceOverloadedException;
import de.uniko.sebschlicht.socialnet.StatusUpdate;

/**
 * node proxy for a status update displayed in news feeds
 * 
 * @author sebschlicht
 * 
 */
public class StatusUpdateProxy extends SocialNodeProxy {

    /**
     * permanent universally unique identifier
     */
    public static final String PROP_IDENTIFIER = "identifier";

    /**
     * date and time the activity was published
     */
    public static final String PROP_PUBLISHED = "published";

    /**
     * content message
     */
    public static final String PROP_MESSAGE = "message";

    /**
     * status update identifier
     */
    protected long identifier;

    /**
     * timestamp of publishing
     */
    protected long published;

    /**
     * author (proxy to user node)
     */
    protected UserProxy pAuthor;

    /**
     * Create a status update node to provide data access and manipulation.
     * 
     * @param nStatusUpdate
     *            status update node to get and set data
     */
    public StatusUpdateProxy(
            Node nStatusUpdate) {
        super(nStatusUpdate);
    }

    /**
     * Initialized the status update node wrapped by this proxy.
     * Sets node properties only.
     * 
     * @param published
     *            timestamp of publishing
     * @param message
     *            status update content
     */
    public void initNode(long published, String message) {
        try {
            long identifier = Muid.create(UidType.DISC).getValue();
            setIdentifier(identifier);
        } catch (ServiceOverloadedException e) {
            throw new IllegalStateException(e);
        }
        setPublished(published);
        setMessage(message);
    }

    public void setAuthor(UserProxy pAuthor) {
        this.pAuthor = pAuthor;
    }

    /**
     * @return status update identifier
     */
    public long getIdentifier() {
        return identifier;
    }

    private void setIdentifier(long identifier) {
        this.identifier = identifier;
        node.setProperty(PROP_IDENTIFIER, identifier);
    }

    public String getMessage() {
        return (String) node.getProperty(PROP_MESSAGE);
    }

    public void setMessage(String message) {
        node.setProperty(PROP_MESSAGE, message);
    }

    /**
     * 
     * @return cached timestamp of publishing
     */
    public long getPublished() {
        if (published == 0) {
            published = (long) node.getProperty(PROP_PUBLISHED);
        }
        return published;
    }

    public void setPublished(long published) {
        this.published = published;
        node.setProperty(PROP_PUBLISHED, published);
    }

    public StatusUpdate getStatusUpdate() {
        return new StatusUpdate(pAuthor.getIdentifier(), getPublished(),
                getMessage());
    }
}
