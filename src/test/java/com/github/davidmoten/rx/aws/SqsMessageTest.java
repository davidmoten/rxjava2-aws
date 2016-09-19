package com.github.davidmoten.rx.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

public class SqsMessageTest {

	@Test
	public void test() {
		SqsMessage m = new SqsMessage("r", new byte[] {}, 1000, Optional.of("123"),
				new SqsMessage.Service(null, null, null, null, null, null));
		assertEquals(1000L, m.lastModifiedTime());
		assertTrue(m.toString().contains("123"));
	}

}
