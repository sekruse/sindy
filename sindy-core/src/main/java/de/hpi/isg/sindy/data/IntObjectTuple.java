package de.hpi.isg.sindy.data;

import java.io.Serializable;

/**
 * This class represents a tuple consisting of an {@code int} and some {@link Object}.
 */
public class IntObjectTuple<B> implements Serializable {

    public int a;

    public B b;

    public IntObjectTuple() {
        this(0, null);
    }

    public IntObjectTuple(int a, B b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public String toString() {
        return String.format("(%d, %s)", this.a, this.b);
    }

}
