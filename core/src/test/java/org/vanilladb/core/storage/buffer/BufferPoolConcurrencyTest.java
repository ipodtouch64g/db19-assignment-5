/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.buffer;

import java.sql.Connection;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.BarrierStartRunner;

import junit.framework.Assert;

public class BufferPoolConcurrencyTest {
	private static Logger logger = Logger.getLogger(BufferPoolConcurrencyTest.class.getName());

	private static final int CLIENT_PER_BLOCK = 100;
	private static final int TEST_BLOCK_COUNT = 10;
	private static final int TOTAL_CLIENT_COUNT = TEST_BLOCK_COUNT * CLIENT_PER_BLOCK;

	private static final String TEST_FILE_NAME = "_tempbufferpooltest";

	@BeforeClass
	public static void init() {
		ServerInit.init(BufferPoolConcurrencyTest.class);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN BUFFER POOL CONCURRENCY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH BUFFER POOL CONCURRENCY TEST");
	}

	@Test
	public void testConcourrentPinning() {
		CyclicBarrier startBarrier = new CyclicBarrier(TOTAL_CLIENT_COUNT);
		CyclicBarrier endBarrier = new CyclicBarrier(TOTAL_CLIENT_COUNT + 1);
		Pinner[] pinners = new Pinner[TOTAL_CLIENT_COUNT];

		// Create multiple threads
		for (int blkNum = 0; blkNum < TEST_BLOCK_COUNT; blkNum++)
			for (int i = 0; i < CLIENT_PER_BLOCK; i++) {
				pinners[blkNum * CLIENT_PER_BLOCK + i] = new Pinner(startBarrier, endBarrier, 
						new BlockId(TEST_FILE_NAME, blkNum));
				pinners[blkNum * CLIENT_PER_BLOCK + i].start();
			}

		// Wait for running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		// Check the results
		for (int blkNum = 0; blkNum < TEST_BLOCK_COUNT; blkNum++) {
			Buffer buffer = pinners[blkNum * CLIENT_PER_BLOCK].buf;
			
			for (int i = 0; i < CLIENT_PER_BLOCK; i++) {
				
				// Check if there is any exception
				if (pinners[blkNum * CLIENT_PER_BLOCK + i].getException() != null)
					Assert.fail("Exception happens: " + pinners[blkNum * CLIENT_PER_BLOCK + i]
							.getException().getMessage());
				
				// The threads using the same block id should get the
				// same buffer
				if (buffer != pinners[blkNum * CLIENT_PER_BLOCK + i].buf)
					Assert.fail("Thread no." + i + " for block no." + blkNum + " get a wrong buffer");
			}
		}
	}

	class Pinner extends BarrierStartRunner {
		
		BufferMgr bufferMgr;
		BlockId blk;
		Buffer buf;

		public Pinner(CyclicBarrier startBarrier, CyclicBarrier endBarrier, BlockId blk) {
			super(startBarrier, endBarrier);
			
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			this.bufferMgr = tx.bufferMgr();
			this.blk = blk;
		}

		@Override
		public void runTask() {
			for (int i = 0; i < 100; i++) {
				buf = bufferMgr.pin(blk);
				bufferMgr.unpin(buf);
			}
			buf = bufferMgr.pin(blk);
		}

	}
}
