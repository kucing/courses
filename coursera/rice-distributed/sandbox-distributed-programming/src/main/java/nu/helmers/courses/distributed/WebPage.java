package nu.helmers.courses.distributed;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

/** A single website in a graph of websites. */
public final class WebPage implements Serializable {
    
    /** Unique ID for this website. */
    private int id;
    
    /** List of destinations from this website (i.e. links to other websites). */
    private LinkedList<Integer> destinations;

    public WebPage(final int pageId) {
        this.id = pageId;
        this.destinations = new LinkedList<Integer>();
    }

    public WebPage() {
        this(-1);
    }

    void addDestination(final int target) {
        destinations.add(target);
    }

    int getId() {
        return id;
    }

    int getDestinationCount() {
        return destinations.size();
    }

    Iterator<Integer> destinations() {
        return destinations.iterator();
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof WebPage) {
            WebPage other = (WebPage) o;
            return other.id == id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
