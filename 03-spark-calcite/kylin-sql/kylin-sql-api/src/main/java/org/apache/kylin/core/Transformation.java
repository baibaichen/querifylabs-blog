package org.apache.kylin.core;

/** ?? */
public class Transformation<T> {

    private final T _input;

    public Transformation(T input) {
        _input = input;
    }

    public T get_input() {
        return _input;
    }
}
