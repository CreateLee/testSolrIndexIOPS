package com.solr.testIOPS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

public class EatDocBuffer implements Runnable {
	private SolrInputDocument[] docs = null;
	private Date start = null;
	private Date end = null;
	private String cloudUrl = null;
	private String collectionName = null;
	private boolean commitNow;
	
	private CountDownLatch threadsSignal = null;  

	
	public EatDocBuffer(SolrInputDocument[] docs, String cloudUrl, String collectionName, CountDownLatch threadsSignal, boolean commitNow) {
		this.docs = docs;
		this.cloudUrl = cloudUrl;
		this.collectionName = collectionName;
		this.threadsSignal = threadsSignal;  
		this.commitNow = commitNow;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		eatDocBufffer();
		
		//System.out.println(Integer.parseInt(Thread.currentThread().getName().split("-")[1]));
		
		IndexingRate.useSeconds[Integer.parseInt(Thread.currentThread().getName().split("-")[1])]= (end.getTime() - start.getTime());
		
		System.out.println(Thread.currentThread().getName() + "\t" + "kaishi:" + start.getMinutes() + " : " + start.getSeconds() + "\t" + "jieshu:" + end.getMinutes() + " : " + end.getSeconds() + "\t" + (end.getTime() - start.getTime()));
		
		threadsSignal.countDown();//线程结束时计数器减1  
		
	}
	
	
	/**
	 * process docs
	 */
	public void eatDocBufffer() {
		CloudSolrClient solrClient = new CloudSolrClient(cloudUrl);

        solrClient.setDefaultCollection(collectionName);

        solrClient.connect();
        
        start = new Date();

		try {
			solrClient.add(Arrays.asList(docs));
			
			// solrClient.commit();
			if (commitNow) {
				solrClient.commit();
			}
		} catch (SolrServerException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			solrClient.shutdown();
		}
		
		
		end = new Date();
		    
	}

}
