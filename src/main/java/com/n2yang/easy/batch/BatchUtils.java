package com.n2yang.easy.batch;

import org.apache.commons.collections4.ListUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

/**
 * 批量处理工具类
 *
 * @author 2yangs
 * @date 2024/04/29
 * @since 1.0
 */
public class BatchUtils {

    private BatchUtils() {}

    /**
     * 数据分组，默认批次大小为1000
     *
     * @param list 待分批数据
     * @return
     * @param <T>
     */
    public static <T> List<List<T>> batchList(final List<T> list) {
        return batchList(list, 1000);
    }

    /**
     * 数据分组
     *
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @return
     * @param <T>
     */
    public static <T> List<List<T>> batchList(final List<T> list, int batchSize) {
        if (null == list || list.isEmpty()) {
            return Collections.emptyList();
        }
        if (list.size() > batchSize) {
            return ListUtils.partition(list, batchSize);
        }
        return Collections.singletonList(list);
    }

    /**
     * 数据分组，默认批次大小为1000
     *
     * @param list 待分批数据
     * @return
     * @param <T>
     */
    public static <T> Stream<List<T>> batchStream(final List<T> list) {
        return batchStream(list, 1000);
    }

    /**
     * 数据分组
     *
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @return
     * @param <T>
     */
    public static <T> Stream<List<T>> batchStream(final List<T> list, int batchSize) {
        if (null == list || list.isEmpty()) {
            return Stream.empty();
        }
        if (list.size() > batchSize) {
            return ListUtils.partition(list, batchSize).stream();
        }
        return Stream.of(list);
    }

    /**
     * 数据分批执行，默认批次大小为1000
     *
     * @param batchRun 分批执行模式
     * @param list 待分批数据
     * @param <T>
     * @throws Exception
     */
    public static <T> void runBatch(BatchRun<T> batchRun, List<T> list) throws Exception {
        Objects.requireNonNull(batchRun);
        batchRun.run(list);
    }

    /**
     * 数据分批执行
     *
     * @param batchRun 分批执行模式
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @param <T>
     * @throws Exception
     */
    public static <T> void runBatch(BatchRun<T> batchRun, List<T> list, int batchSize) throws Exception {
        Objects.requireNonNull(batchRun);
        batchRun.run(list, batchSize);
    }

    /**
     * 数据分批执行，默认批次大小为1000
     *
     * @param batchSupply 分批执行模式
     * @param list 待分批数据
     * @return
     * @param <T>
     * @param <R>
     * @throws Exception
     */
    public static <T, R> List<R> supplyBatch(BatchSupply<T, R> batchSupply, List<T> list) throws Exception {
        Objects.requireNonNull(batchSupply);
        return batchSupply.supply(list);
    }

    /**
     * 数据分批执行
     *
     * @param batchSupply 分批执行模式
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @return
     * @param <T>
     * @param <R>
     * @throws Exception
     */
    public static <T, R> List<R> supplyBatch(BatchSupply<T, R> batchSupply, List<T> list,
                                             int batchSize) throws Exception {
        Objects.requireNonNull(batchSupply);
        return batchSupply.supply(list, batchSize);
    }

    /**
     * 数据分批异步执行，默认批次大小为1000
     *
     * @param batchRun 分批执行模式
     * @param list 待分批数据
     * @param <T>
     * @throws Exception
     */
    public static <T> void runAsync(BatchRun<T> batchRun, List<T> list) throws Exception {
        Objects.requireNonNull(batchRun);
        batchRun.runAsync(list);
    }

    /**
     * 数据分批异步执行
     *
     * @param batchRun 分批执行模式
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @param <T>
     * @throws Exception
     */
    public static <T> void runAsync(BatchRun<T> batchRun, List<T> list, int batchSize) throws Exception {
        Objects.requireNonNull(batchRun);
        batchRun.runAsync(list, batchSize);
    }

    /**
     * 数据分批异步执行，默认批次大小为1000
     *
     * @param batchRun 分批执行模式
     * @param list 待分批数据
     * @param <T>
     * @throws Exception
     */
    public static <T> void runAsync(BatchRun<T> batchRun, List<T> list, Executor executor) throws Exception {
        Objects.requireNonNull(batchRun);
        batchRun.runAsync(list, executor);
    }

    /**
     * 数据分批异步执行
     *
     * @param batchRun 分批执行模式
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @param <T>
     * @throws Exception
     */
    public static <T> void runAsync(BatchRun<T> batchRun, List<T> list, int batchSize,
                                    Executor executor) throws Exception {
        Objects.requireNonNull(batchRun);
        batchRun.runAsync(list, batchSize, executor);
    }

    /**
     * 数据分批异步执行，默认批次大小为1000
     *
     * @param batchSupply 分批执行模式
     * @param list 待分批数据
     * @return
     * @param <T>
     * @param <R>
     * @throws Exception
     */
    public static <T, R> List<R> supplyAsync(BatchSupply<T, R> batchSupply, List<T> list) throws Exception {
        Objects.requireNonNull(batchSupply);
        return batchSupply.supplyAsync(list);
    }

    /**
     * 数据分批异步执行
     *
     * @param batchSupply 分批执行模式
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @return
     * @param <T>
     * @param <R>
     * @throws Exception
     */
    public static <T, R> List<R> supplyAsync(BatchSupply<T, R> batchSupply, List<T> list,
                                             int batchSize) throws Exception {
        Objects.requireNonNull(batchSupply);
        return batchSupply.supplyAsync(list, batchSize);
    }

    /**
     * 数据分批异步执行，默认批次大小为1000
     *
     * @param batchSupply 分批执行模式
     * @param list 待分批数据
     * @return
     * @param <T>
     * @param <R>
     * @throws Exception
     */
    public static <T, R> List<R> supplyAsync(BatchSupply<T, R> batchSupply, List<T> list,
                                             Executor executor) throws Exception {
        Objects.requireNonNull(batchSupply);
        return batchSupply.supplyAsync(list, executor);
    }

    /**
     * 数据分批异步执行
     *
     * @param batchSupply 分批执行模式
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @return
     * @param <T>
     * @param <R>
     * @throws Exception
     */
    public static <T, R> List<R> supplyAsync(BatchSupply<T, R> batchSupply, List<T> list,
                                             int batchSize, Executor executor) throws Exception {
        Objects.requireNonNull(batchSupply);
        return batchSupply.supplyAsync(list, batchSize, executor);
    }
}
