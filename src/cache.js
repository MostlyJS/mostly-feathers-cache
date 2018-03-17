import assert from 'assert';
import crypto from 'crypto';
import makeDebug from 'debug';
import fp from 'mostly-func';
import { helpers } from 'mostly-feathers-mongoose';
import util from 'util';

const debug = makeDebug('mostly:feathers-cache:cache');

const defaultOptions = {
  enabled: true,
  idField: 'id',
  keyPrefix: 'mostly:cache:',
  headers: [],
  perUser: false,
  ttl: 600 // seconds
};

/**
 * IMPORTANT CONSTRAINTS
 * Currently cacheMap must be stand-alone cache (like Redis) for each service,
 * otherwise cache cannot be deleted properly and may be stalled.
 *
 * Another issue will be caching with populate/assoc fields, we need to find a
 * possible way to invalidate cache when the populated data changed.
 *
 * Cache structure
 * - service
 *   - lastWrite: time
 *   - queryKey: value
 * - id
 *   - lastWrite: time
 *   - queryKey: value
 */
export default function (...opts) {
  opts = fp.assign(defaultOptions, ...opts);
  assert(opts.name, 'app setting of cache is not found, check your app configuration');

  return async function (context) {
    if (!opts.enabled) return;

    const cacheMap = context.app.get(opts.name);
    assert(cacheMap, `app setting '${opts.name}' must be provided`);

    const idField = opts.idField || (context.service || {}).id;
    const svcName = (context.service || {}).name;
    const svcTtl = (opts.strategies || {})[svcName] || opts.ttl;

    const svcKey = opts.keyPrefix + svcName;

    // generate a unique key for query with same params
    const genQueryKey = (context, id) => {
      const headers = fp.pickPath(opts.headers || [], context.params.headers || {});
      const hash = crypto.createHash('md5')
        .update(context.path)
        .update(context.method)
        .update(JSON.stringify(context.params.query || {}))
        .update(context.params.__action || '')
        .update(context.params.provider || '')
        .update(headers && fp.values(headers).join(''))
        .update(opts.perUser && context.params.user && context.params.user.id || '')
        .digest('hex');
      return opts.keyPrefix + (id? id + ':' + hash : hash);
    };

    // get query result from cacheMap and check lastWrite
    const getCacheValue = async function (svcKey, idKey, queryKey) {
      const results = await cacheMap.multi(svcKey, idKey, queryKey);
      const svcMeta = results[0] && JSON.parse(results[0]);
      const idMeta = results[1] && JSON.parse(results[1]);
      const cacheValue = results[2] && JSON.parse(results[2]);

      // special cache miss where it is out of date
      if (cacheValue) {
        let outdated = false;
        if (svcMeta) {
          outdated = outdated || cacheValue.metadata.lastWrite < svcMeta.lastWrite;
        }
        if (idMeta) {
          outdated = outdated || cacheValue.metadata.lastWrite < idMeta.lastWrite;
        }
        if (svcMeta && idMeta) {
          outdated = outdated || idMeta.lastWrite < svcMeta.lastWrite;
        }
        if (outdated) {
          debug(`<< ${svcKey} out of date: ${queryKey}`);
          return null;
        } else {
          debug(`<< ${svcKey} hit cache: ${queryKey}`);
          return fp.dissocPath(['metadata', 'lastWrite'], cacheValue);
        }
      } else {
        debug(`<< ${svcKey} miss cache: ${queryKey}`);
        return null;
      }
    };

    const setCacheValue = async function (svcKey, queryKey, value, ttl) {
      let metadata = { lastWrite: Date.now() };
      let data = value;
      let message = '';

      if (value && value.data) {
        metadata = fp.assign(metadata, value.metadata || fp.omit(['data'], value));
        data = value.data;
        message = value.message || '';
      }
      debug(`>> ${svcKey} set cache: ${queryKey}`);
      return cacheMap.set(queryKey, JSON.stringify({ message, metadata, data }));
    };

    const touchService = async function (nameKey) {
      debug(`>> ${svcKey} touched ${nameKey}: ` + Date.now());
      return cacheMap.set(nameKey, JSON.stringify({
        lastWrite: Date.now()
      }));
    };

    const addCacheHits = (queryKey) => {
      context.cacheHits = (context.cacheHits || []).concat(queryKey);
    };

    if (context.type === 'after') {

      const saveForCache = async function (svcKey, id, value) {
        const queryKey = genQueryKey(context, id);
        if (!fp.contains(queryKey, context.cacheHits || [])) {
          await setCacheValue(svcKey, queryKey, value, svcTtl);
        }
      };

      switch (context.method) {
        case 'find': {
          await saveForCache(svcKey, null, context.result);
          break;
        }
        case 'get': {
          // save for cache
          if (context.id) {
            const item = helpers.getHookData(context);
            if (item.id !== context.id) {
              // save as virutal id like username, path, etc
              await saveForCache(svcKey, context.id, item);
            } else {
              await saveForCache(svcKey, item[idField], item);
            }
          }
          break;
        }
        default: { // update, patch, remove
          const items = helpers.getHookDataAsArray(context);
          for (const item of items) {
            const idKey = opts.keyPrefix + item[idField];
            await touchService(idKey);
          }
        }
      }

    } else {

      switch (context.method) {
        case 'find': {
          const queryKey = genQueryKey(context);
          const values = await getCacheValue(svcKey, null, queryKey);
          if (values) {
            addCacheHits(queryKey);
            context.result = values;
          }
          break;
        }
        case 'create':
          break;
        case 'get': {
          if (context.id) {
            const idKey = opts.keyPrefix + context.id;
            const queryKey = genQueryKey(context, context.id);
            const value = await getCacheValue(svcKey, idKey, queryKey);
            if (value) {
              addCacheHits(queryKey);
              context.result = value.data;
            }
          }
          break;
        }
        default: { // update, patch, remove
          if (context.id) {
            const idKey = opts.keyPrefix + context.id;
            await Promise.all([
              touchService(svcKey),
              touchService(idKey)
            ]);
          } else {
            await touchService(svcKey);
          }
        }
      }
    }

    return context;
  };
}