package dev.vality.fistful.magista.handler;

public interface EventHandler<T, E> {

    boolean accept(T change);

    void handle(T change, E event);

}
