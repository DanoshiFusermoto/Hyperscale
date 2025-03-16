package org.radix.hyperscale.utils;

@FunctionalInterface
public interface TriConsumer<T, U, W> {

  /**
   * Applies this function to the given arguments.
   *
   * @param t the first function argument
   * @param u the second function argument
   * @param w the third function argument
   */
  void apply(T t, U u, W w);
}
