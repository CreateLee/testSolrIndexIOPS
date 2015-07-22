package com.solr.testIOPS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

import com.solr.testIOPS.utils.Random;

/**
 * 
 * @author kailee
 * @email likaiwen@163.com
 *
 */
public class IndexingRate {
	// indexing入库完毕与测试数据删除action中间间隔时间
	public static int seconds = 10;
	public static long[] useSeconds = null;
	
	public static void main(String[] args) {
		// 连接zookeeper的url
		String cloudUrl = "172.37.37.36:2181,172.37.37.37:2181,172.37.37.38:2181";
		// solrcloud collection 的名字
		String collectionName = "hbase";
		// dynamic field 后缀
		String dynamicSubfix = "_t";
		// doc id
		String rowkey = "rowkey";
		// 执行doc测试时，是复杂测试还是简单，建议复杂测试与简单对比
		boolean isComplex = true;
		// 执行测试的线程数量，建议根据具体主机的 多少 core 来合理测试
		int threadNum = 4;
		// list doc 的 数量，即每次向 solrcloud 提交多少个doc
		int doclistSize = 10000;
		boolean commitNow = true;
		// doc 的 id 前缀，方便对测试的doc数据进行删除，建议根据具体情况合理设置，因为删除是delete.byquery(rowkey:idPrefix*);
		/**
		 * 为避免删除您的业务数据，请您谨慎 设置
		 */
		String idPrefix = "rowkeyAsIDIOPStest";

		SolrInputDocument docss[][] = new SolrInputDocument[threadNum][doclistSize];

		System.out.println("commitNow: " + commitNow + "\tisComplex: " + isComplex + "\tthreadNum: " + threadNum + "\tdoclistSize: " + doclistSize);
		
		trunk(cloudUrl, collectionName, dynamicSubfix, rowkey, isComplex,
				threadNum, doclistSize, docss, idPrefix, commitNow);
		
		long maxUseSecond = 0;
		
		for (long second : useSeconds) {
			if (second > maxUseSecond) {
				maxUseSecond = second;
			}
		}
		
		try {
			System.out.println("commitNow: " + commitNow + "\tisComplex: " + isComplex + "\tthreadNum: " + threadNum + "\tdoclistSize: " + doclistSize);

			System.out.println("in those test conditions, indexing rate is : " + ((threadNum*doclistSize*1000)/maxUseSecond) + "\t doc per seconds");
		} catch (ArithmeticException e) {
			// TODO: handle exception
			System.out.println("计算 indexing rate 失败！");
		}
		
		
		System.out.println("thanks your working !!!!!\t your friend kaiwebLee");

	}

	public static void trunk(String cloudUrl, String collectionName,
			String dynamicSubfix, String rowkey, boolean isComplex,
			int threadNum, int doclistSize, SolrInputDocument docss[][], String idPrefix, boolean commitNow) {
		useSeconds = new long[threadNum+1];

		CountDownLatch countDownLatch = new CountDownLatch(threadNum);

		Date begin = new Date();

		for (int i = 0; i < threadNum; i++) {
			for (int j = 0; j < doclistSize; j++) {
				SolrInputDocument doc = new SolrInputDocument();

				doc.addField(rowkey, idPrefix + j + "_" + i);
				doc.addField("i" + dynamicSubfix, "" + i);
				doc.addField("j" + dynamicSubfix, "" + j);

				if (isComplex) {
					int k = 0;
					while (k++ < 10) {
						Map<String, String> set = new HashMap<String, String>();
						set.put("set", getRandomStr());
						doc.addField("key" + k + dynamicSubfix, set);
					}
				}

				docss[i][j] = doc;
			}

			new Thread(new EatDocBuffer(docss[i], cloudUrl, collectionName,
					countDownLatch, commitNow)).start();
		}

		Date stop = new Date();

		System.out.println(stop.getTime() - begin.getTime());

		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// 等待所有子线程执行完

		System.out.println("即将删除测试数据……");
		
		try {
			Thread.sleep(1000*seconds);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (idPrefix==null || idPrefix.length() < 12) {
			System.out.println(idPrefix + "\t为空或过于简单,为了您的数据安全,\n删除测试数据动作未执行，请您手动删除，非常抱歉");
		} else {
			delTestData(cloudUrl, collectionName, rowkey, idPrefix);
		}
		
	}

	public static void delTestData(String cloudUrl, String collectionName,
			String rowkey, String idPrefix) {
		// TODO Auto-generated method stub
		CloudSolrClient client = new CloudSolrClient(cloudUrl);

		client.setDefaultCollection(collectionName);

		client.connect();

		try {
			
			client.deleteByQuery(rowkey + ":" + idPrefix+"*");

			client.commit();
		} catch (SolrServerException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		client.shutdown();
	}

	public static String getRandomStr() {
		return new String(build(64));
	}

	public static char[] build(int length) {
		char[] captcha = new char[32];

		Random random = new Random();

		if (captcha.length != length) {
			captcha = new char[length];
		}

		for (int i = 0; i < length; i++) {
			captcha[i] = (char) random.nextInt(0x53E3, 0x559D);
		}
		return captcha;
	}

}
