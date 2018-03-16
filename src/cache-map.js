import LruCache from 'lru-cache';
import fp from 'mostly-func';

const defaultOptions = {
  max: 500,
  maxAge: 1000 * 60 * 10
};

export class CacheMap {
  constructor(options) {
    options = fp.assign(defaultOptions, options);
    this._cache = new LruCache(options);
  }

  async get(id) {
    return id && this._cache.get(id);
  }

  async multi(...args) {
    return Promise.all(fp.map(async (id) => this.get(id), args));
  }

  async set(id, val, ttl) {
    return this._cache.set(id, val, ttl * 1000);
  }

  async delete(id) {
    return this._cache.del(id);
  }

  async clear() {
    return this._cache.reset();
  }
}

export default function init (options) {
  return new CacheMap(options);
}
