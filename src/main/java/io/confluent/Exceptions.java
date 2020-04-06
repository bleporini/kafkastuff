package io.confluent;

public class Exceptions {

    public interface CheckedSupplier<T, E extends Exception>{
        T get() throws E;
    }

    public interface CheckedRunnable<T, E extends Exception>{
        void run() throws E;
    }

    public static <T, E extends Exception> T uncheckIt(CheckedSupplier<T, E> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T, E extends Exception> void uncheckIt(CheckedRunnable<T, E> runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
