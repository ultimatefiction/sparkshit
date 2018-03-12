package org.veritas.training.spark;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedissonClientProvider {

    public static final int DATABASE = 0;
    public static final String ADDRESS = "127.0.0.1:6379";
    public static final String PASSWORD = "msxfpwd";

    public static RedissonClient getClient() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress(ADDRESS)
                .setPassword(PASSWORD)
                .setDatabase(DATABASE);
        return Redisson.create(config);
    }

}
