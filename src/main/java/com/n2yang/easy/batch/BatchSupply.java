package com.n2yang.easy.batch;

import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * 批量处理接口，有返回值
 *
 * @author 2yangs
 * @date 2024/04/29
 * @since 1.0
 */
@FunctionalInterface
public interface BatchSupply<T, R> {

    /**
     * 分批处理
     *
     * @param list 分批数据
     * @return
     * @throws Exception
     */
    List<R> batch(List<T> list) throws Exception;

    /**
     * 准备分批处理，批次大小默认1000
     *
     * @param list 待分批数据
     * @return
     * @throws Exception
     */
    default List<R> supply(List<T> list) throws Exception {
        return supply(list, 1000);
    }

    /**
     * 准备分批处理
     *
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @return
     * @throws Exception
     */
    default List<R> supply(List<T> list, int batchSize) throws Exception {
        Assert.notEmpty(list, "list can not be empty");
        if (list.size() > batchSize) {
            List<List<T>> batchedList = BatchUtils.batchList(list, batchSize);
            List<R> result = new ArrayList<>(list.size());
            for (List<T> currentList : batchedList) {
                Optional.ofNullable(batch(currentList)).ifPresent(result::addAll);
            }
            return result;
        } else {
            return batch(list);
        }
    }

    /**
     * 准备分批异步处理，批次大小默认1000
     *
     * @param list 待分批数据
     * @throws Exception
     */
    default List<R> supplyAsync(List<T> list) throws Exception {
        return supplyAsync(list, 1000);
    }

    /**
     * 准备分批异步处理
     *
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @throws Exception
     */
    default List<R> supplyAsync(List<T> list, int batchSize) throws Exception {
        Assert.notEmpty(list, "list can not be empty");
        if (list.size() > batchSize) {
            List<List<T>> batchedList = BatchUtils.batchList(list, batchSize);
            List<CompletableFuture<List<R>>> futureList = new ArrayList<>(batchedList.size());
            for (List<T> currentList : batchedList) {
                CompletableFuture<List<R>> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        return batch(currentList);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                futureList.add(future);
            }
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[]{})).join();
            List<R> result = new ArrayList<>(futureList.size());
            for (CompletableFuture<List<R>> future : futureList) {
                Optional.ofNullable(future.get()).ifPresent(result::addAll);
            }
            return result;
        } else {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return batch(list);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).get();
        }
    }

    /**
     * 准备分批异步处理，批次大小默认1000
     *
     * @param list 待分批数据
     * @throws Exception
     */
    default List<R> supplyAsync(List<T> list, Executor executor) throws Exception {
        return supplyAsync(list, 1000, executor);
    }

    /**
     * 准备分批异步处理
     *
     * @param list 待分批数据
     * @param batchSize 批次大小
     * @throws Exception
     */
    default List<R> supplyAsync(List<T> list, int batchSize, Executor executor) throws Exception {
        Assert.notEmpty(list, "list can not be empty");
        Objects.requireNonNull(executor);
        if (list.size() > batchSize) {
            List<List<T>> batchedList = BatchUtils.batchList(list, batchSize);
            List<CompletableFuture<List<R>>> futureList = new ArrayList<>(batchedList.size());
            for (List<T> currentList : batchedList) {
                CompletableFuture<List<R>> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        return batch(currentList);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor);
                futureList.add(future);
            }
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[]{})).join();
            List<R> result = new ArrayList<>(futureList.size());
            for (CompletableFuture<List<R>> future : futureList) {
                Optional.ofNullable(future.get()).ifPresent(result::addAll);
            }
            return result;
        } else {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return batch(list);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor).get();
        }
    }
}
