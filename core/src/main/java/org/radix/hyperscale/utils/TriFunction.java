package org.radix.hyperscale.utils;

@FunctionalInterface
public interface TriFunction<T, U, W, R> {

    /**
     * Applies this function to the given arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @param w the third function argument
     * @return the function result
     */
    R apply(T t, U u, W w);
}