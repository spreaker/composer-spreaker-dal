<?php

/**
 * A naive impelmentation used for testcases of DAL
 */

use Spreaker\Dal\Cache\CacheInterface as CacheInterface;

class DalArrayCache implements CacheInterface
{
    /**
     * @var Array
     */
    private $_cachedData = array();

    /**
     * delete all keys from cache
     */
    public function deleteAllKeys()
    {
        return $this->_cachedData = array();
    }

    /**
     * @see Spreaker\Cache\CacheInterface
     */
    public function get($key)
    {
        if (!$this->has($key)) {
            return null;
        }

        return $this->_cachedData[$key]['data'];
    }

    /**
     * @see Spreaker\Cache\CacheInterface
     */
    public function set($key, $data, $ttl = null)
    {
        $expires_at = time();
        if (is_null($ttl)) {
            $expires_at = PHP_INT_MAX;
        } else {
            $expires_at = $expires_at + $ttl;
        }

        $this->_cachedData[$key] = array(
            'data'       => $data,
            'expires_at' => $expires_at,
        );

        return true;
    }

    /**
     * @see Spreaker\Cache\CacheInterface
     */
    public function del($key)
    {
        unset($this->_cachedData[$key]);

        return true;
    }

    /**
     * @see Spreaker\Cache\CacheInterface
     */
    public function has($key)
    {
        return isset($this->_cachedData[$key]) && $this->_cachedData[$key]['expires_at'] > time();
    }
}
