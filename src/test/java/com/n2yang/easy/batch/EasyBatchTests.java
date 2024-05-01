package com.n2yang.easy.batch;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 批量处理接口，无返回值
 *
 * @author 2yangs
 * @date 2024/05/01
 * @since 1.0
 */
public class EasyBatchTests {

	@Test
	public void runBatchTest() throws Exception {
		// 分批执行代码块
		BatchRun<Integer> batchRun = list -> {
			System.out.println("执行分批的数据量：" + list.size());
		};
		// 初始数据
		List<Integer> integers = Stream.iterate(0, i -> i + 1).limit(9999)
				.collect(Collectors.toList());
		System.out.println("初始数据量：" + integers.size());
		BatchUtils.runBatch(batchRun, integers);
	}

	@Test
	public void supplyBatchTest() throws Exception {
		// 分批执行代码块
		BatchSupply<Integer, String> batchSupply = list -> {
			System.out.println("执行分批的数据量：" + list.size());
			return list.stream().map(String::valueOf).collect(Collectors.toList());
		};
		// 初始数据
		List<Integer> integers = Stream.iterate(0, i -> i + 1).limit(9999)
				.collect(Collectors.toList());
		System.out.println("初始数据量：" + integers.size());
		List<String> result = BatchUtils.supplyBatch(batchSupply, integers);
		System.out.println("返回数据量：" + result.size());
	}

	@Test
	public void runAsyncTest() throws Exception {
		// 分批执行代码块
		BatchRun<Integer> batchRun = list -> {
			System.out.println(Thread.currentThread().getName() + "执行分批的数据量：" + list.size());
		};
		// 初始数据
		List<Integer> integers = Stream.iterate(0, i -> i + 1).limit(9999)
				.collect(Collectors.toList());
		System.out.println("初始数据量：" + integers.size());
		BatchUtils.runAsync(batchRun, integers);
	}

	@Test
	public void supplyAsyncTest() throws Exception {
		// 分批执行代码块
		BatchSupply<Integer, String> batchSupply = list -> {
			System.out.println(Thread.currentThread().getName() + "执行分批的数据量：" + list.size());
			return list.stream().map(String::valueOf).collect(Collectors.toList());
		};
		// 初始数据
		List<Integer> integers = Stream.iterate(0, i -> i + 1).limit(9999)
				.collect(Collectors.toList());
		long start = System.currentTimeMillis();
		System.out.println("初始数据量：" + integers.size());
		List<String> result = BatchUtils.supplyAsync(batchSupply, integers);
		System.out.println("返回数据量：" + result.size() + "，用时秒：" + (System.currentTimeMillis() - start)/1000d);
	}

}
