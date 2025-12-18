package com.blazecache;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

public class CacheClientTest {

    @Test
    public void testRoundRobinSelection() throws Exception {
        CacheClient c = new CacheClient(
                Arrays.asList("A", "B", "C"),
                CacheClient.SelectionStrategy.ROUND_ROBIN
        );

        Method select = CacheClient.class.getDeclaredMethod("selectServer", String.class);
        select.setAccessible(true);

        String[] got = new String[6];
        for (int i = 0; i < 6; i++) {
            got[i] = (String) select.invoke(c, "k");
        }
        Assert.assertArrayEquals(new String[]{"A","B","C","A","B","C"}, got);
    }


    @Test
    public void testConsistentHashingDeterminism() throws Exception {
        CacheClient c = new CacheClient(
                Arrays.asList("A", "B"),
                CacheClient.SelectionStrategy.CONSISTENT_HASHING
        );

        Method select = CacheClient.class.getDeclaredMethod("selectServer", String.class);
        select.setAccessible(true);

        String s1 = (String) select.invoke(c, "alpha");
        String s2 = (String) select.invoke(c, "alpha");
        Assert.assertEquals(s1, s2);
        Assert.assertTrue(s1.equals("A") || s1.equals("B"));
    }
}
