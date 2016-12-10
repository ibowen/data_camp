package hbase;

import org.apache.hadoop.hbase.client.HTableInterface;

public interface HTableFactoryInterface
{
    HTableInterface checkLapdTable();
    HTableInterface createLapdTable();
    
    /**
     * Cleanup any resources held opened during the table's lifetime
     * Call this after the closing the table
     */
    void cleanup();
}
