package com.github.davidmoten.rx.aws;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class UtilTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Util.class);
    }

}
