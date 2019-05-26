package com.cent.timer.util;

import java.util.function.Supplier;

public class ThreadLocalObject<T> {
    
    private Supplier<T> supplier;
    
    public ThreadLocalObject(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    private ThreadLocal<T> holder = new ThreadLocal<>();

    public T take() {
        T o = holder.get();
        if (o == null) {
            o = supplier.get();
            holder.set(o);
        }
        return o;
    }

}
