package com.github.davidmoten.rx2.aws;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.s3.AmazonS3Client;
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
    
    @Test
    public void testShutdownS3() {
        AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        Mockito.doThrow(new RuntimeException()).when(s3).shutdown();
        // should not throw
        Util.shutdown(s3);   
        Mockito.verify(s3, Mockito.times(1)).shutdown();
    }
}
