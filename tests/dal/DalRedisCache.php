<?php

/**
 * A naive impelmentation used for testcases of DAL
 *
 * In order to use this implementation, you need:
 *  - add predis dependency to tests/composer.json
 *  - have a usable redis instance
 */

use Spreaker\Cache\CacheInterface as CacheInterface;

class DalRedisCache implements CacheInterface
{
    /**
     * @var Predis\Client
     */
    private $_redis = null;

    /**
     * @param Predis\Client $redis  an instance of Predis\Client
     */
    public function __construct(Predis\Client $redis)
    {
        $this->_redis = $redis;
    }

    /**
     * delete all keys from currently selected db
     */
    public function deleteAllKeys()
    {
        return $this->_redis->flushdb();
    }

    /**
     * @see Spreaker\Cache\CacheInterface
     */
    public function get($key)
    {
        return $this->_redis->get($key);
    }

    /**
     * @see Spreaker\Cache\CacheInterface
     */
    public function set($key, $data, $ttl = null)
    {
        if (is_null($ttl)) {
            return $this->_redis->set($key, $data);
        } else {
            return $this->_redis->setex($key, $ttl, $data);
        }
    }

    /**
     * @see Spreaker\Cache\CacheInterface
     */
    public function del($key)
    {
        return (bool) $this->_redis->del($key);
    }

    /**
     * @see Spreaker\Cache\CacheInterface
     */
    public function has($key)
    {
        return $this->_redis->exists($key);
    }
}
