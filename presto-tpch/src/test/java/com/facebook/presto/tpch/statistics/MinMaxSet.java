package com.facebook.presto.tpch.statistics;

import java.util.Optional;

public class MinMaxSet<E>
{
    private Optional<E> min = Optional.empty();
    private Optional<E> max = Optional.empty();

    public MinMaxSet()
    {
    }

    public Optional<E> getMin()
    {
        return min;
    }

    public Optional<E> getMax()
    {
        return max;
    }

    public void add(E ele)
    {
        final Comparable<? super E> castedEle = (Comparable<? super E>) ele;
        if (!min.isPresent() || castedEle.compareTo(min.get()) <= 0) {
            min = Optional.of(ele);
        }
        if (!max.isPresent() || castedEle.compareTo(max.get()) >= 0) {
            max = Optional.of(ele);
        }
    }
}
