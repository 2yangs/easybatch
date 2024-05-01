package com.n2yang.easy.batch;

import org.springframework.util.Assert;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * 批量处理接口，无返回值
 *
 * @author 2yangs
 * @date 2024/04/29
 * @since 1.0
 */
@FunctionalInterface
public interface BatchRun<T> {

    /**
     * 分批处理
     *
     * @param list 分批数据
     * @throws Exception
     */
    void batch(List<T> list) throws Exception;

    /**
     * 准备分批处理，批次大小默认1000
     *
     * @param list 待分批数据
     * @throws Exception
     */
    default void run(List<T> list) throws Exception {
        run(list, 1000);
    }

    /**
     * 准备分批处理
     *
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @throws Exception
     */
    default void run(List<T> list, int batchSize) throws Exception {
        Assert.notEmpty(list, "list can not be empty");
        if (list.size() > batchSize) {
            List<List<T>> lists = BatchUtils.batchList(list, batchSize);
            for (List<T> currentList : lists) {
                batch(currentList);
            }
        } else {
            batch(list);
        }
    }

    /**
     * 准备分批异步处理，批次大小默认1000
     *
     * @param list 待分批数据
     * @throws Exception
     */
    default void runAsync(List<T> list) throws Exception {
        runAsync(list, 1000);
    }

    /**
     * 准备分批异步处理
     *
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @throws Exception
     */
    default void runAsync(List<T> list, int batchSize) throws Exception {
        Assert.notEmpty(list, "list can not be empty");
        if (list.size() > batchSize) {
            List<List<T>> batchedList = BatchUtils.batchList(list, batchSize);
            for (List<T> currentList : batchedList) {
                CompletableFuture.runAsync(() -> {
                    try {
                        batch(currentList);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } else {
            CompletableFuture.runAsync(() -> {
                try {
                    batch(list);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    /**
     * 准备分批异步处理，批次大小默认1000
     *
     * @param list 待分批数据
     * @throws Exception
     */
    default void runAsync(List<T> list, Executor executor) throws Exception {
        runAsync(list, 1000, executor);
    }

    /**
     * 准备分批异步处理
     *
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @throws Exception
     */
    default void runAsync(List<T> list, int batchSize, Executor executor) throws Exception {
        Assert.notEmpty(list, "list can not be empty");
        Objects.requireNonNull(executor);
        if (list.size() > batchSize) {
            List<List<T>> batchedList = BatchUtils.batchList(list, batchSize);
            for (List<T> currentList : batchedList) {
                CompletableFuture.runAsync(() -> {
                    try {
                        batch(currentList);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor);
            }
        } else {
            CompletableFuture.runAsync(() -> {
                try {
                    batch(list);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor);
        }
    }
}
