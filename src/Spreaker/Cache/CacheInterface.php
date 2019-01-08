<?php

namespace Spreaker\Cache;

interface CacheInterface
{
    /**
     * @param string    $key    cache key
     * @param mixed     Returns either the cached data or null
     */
    public function get($key);

    /**
     * @param string    $key    cache key
     * @param string    $data   data to save
     * @param int|null  $ttl    if int, set ttl to specified seconds, if null set ttl to infinite
     * @return boolean  true if no problem
     */
    public function set($key, $data, $ttl = null);

    /**
     * @param string    $key    cache key
     * @return boolean  true if no problem
     */
    public function del($key);

    /**
     * @param string    $key    cache key
     * @return boolean  true if key exists, false otherwise
     */
    public function has($key);
}
