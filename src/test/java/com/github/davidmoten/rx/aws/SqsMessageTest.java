package com.github.davidmoten.rx.aws;

import java.util.Optional;

import org.junit.Test;

public class SqsMessageTest {

	@Test
	public void test() {
		new SqsMessage("r", new byte[] {}, 1000, Optional.of("123"),
				new SqsMessage.Service(null, null, null, null, null, null));
	}

}
