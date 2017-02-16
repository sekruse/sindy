package de.hpi.isg.sindy.io;

/**
 * This interface describes consumers of {@link RemoteCollector} implementations.
 */
public interface RemoteCollectorConsumer<T> {
    void collect(T element);
}
