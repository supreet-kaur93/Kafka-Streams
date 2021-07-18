package handler;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;

import java.util.Map;

public class CustomRockDbConfig implements RocksDBConfigSetter {


    @Override
    public void setConfig(String s, Options options, Map<String, Object> map) {

        // Reduce block size from default
        // default BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
        BlockBasedTableConfig tableConfig = new org.rocksdb.BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(16 * 1024 * 1024L);

        tableConfig.setBlockSize(16 * 1024L);

        // donot let index and filter blocks grow unbounded
        tableConfig.setCacheIndexAndFilterBlocks(true);
        options.setTableFormatConfig(tableConfig);

        options.setMaxWriteBufferNumber(2);

    }
}
