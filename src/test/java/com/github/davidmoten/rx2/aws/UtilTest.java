package com.github.davidmoten.rx2.aws;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class UtilTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Util.class);
    }
    
    @Test
    public void testUncheckedCall() {
        assertEquals(1, (int) Util.uncheckedCall(() -> 1));   
    }
    
    @Test
    public void testUncheckedCallThrowsRuntimeException() {
        try {
            Util.uncheckedCall(() -> {
                throw new IllegalArgumentException("boo");
            });
            Assert.fail();
        } catch (RuntimeException e) {
            assertEquals("boo", e.getMessage());
        }
    }
    
    @Test
    public void testUncheckedCallThrowsError() {
        try {
            Util.uncheckedCall(() -> {
                throw new Error("boo");
            });
            Assert.fail();
        } catch (Error e) {
            assertEquals("boo", e.getMessage());
        }
    }
    
    @Test
    public void testUncheckedCallThrowsCheckedException() {
        try {
            Util.uncheckedCall(() -> {
                throw new Exception("boo");
            });
            Assert.fail();
        } catch (RuntimeException e) {
            assertEquals("boo", e.getCause().getMessage());
        }
    }
    
}
