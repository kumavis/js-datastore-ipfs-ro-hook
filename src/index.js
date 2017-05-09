/* @flow */
'use strict'

/* :: import type {Callback, Batch, Query, QueryResult, QueryEntry} from 'interface-datastore' */

const parallel = require('async/parallel')
const CID = require('cids')
const base32 = require('base32.js')

function dsKeyToCid(key) {
  const decoder = new base32.Decoder()
  const rawKey = key.toString().slice(1)
  const buf = decoder.write(rawKey).finalize()
  const cid = new CID(buf)
  return cid
}

/**
 * A datastore backed by an ipfs client accessed via the ipfs http api.
 */

class IpfsReadOnlyHookedDatastore {
  constructor (getHandler) {
    this.handler = getHandler
  }

  open (callback) {
    setTimeout(callback)
  }

  put (key /* : Key */, value /* : Buffer */, callback /* : Callback<void> */) /* : void */ {
    setTimeout(callback)
  }

  get (key /* : Key */, callback /* : Callback<Buffer> */) /* : void */ {
    const cid = dsKeyToCid(key)
    this.handler(cid, callback)
  }

  has (key /* : Key */, callback /* : Callback<bool> */) /* : void */ {
    this.get(key, (err) => {
      if (err) {
        callback(null, false)
        return
      }

      callback(null, true)
    })
  }

  delete (key /* : Key */, callback /* : Callback<void> */) /* : void */ {
    setTimeout(callback)
  }

  close (callback /* : Callback<void> */) /* : void */ {
    setTimeout(callback)
  }

  batch () /* : Batch<Buffer> */ {
    const ops = []
    const self = this
    return {
      put: (key /* : Key */, value /* : Buffer */) /* : void */ => {
        ops.push((callback) => self.put(key, value, callback))
      },
      delete: (key /* : Key */) /* : void */ => {
        ops.push((callback) => self.delete(key, callback))
      },
      commit: (callback /* : Callback<void> */) /* : void */ => {
        parallel(ops, callback)
      }
    }
  }

  query (q /* : Query<Buffer> */) /* : QueryResult<Buffer> */ {
    throw new Error('IpfsReadOnlyHookedDatastore - "query" method not supported')
  }
}

module.exports = IpfsReadOnlyHookedDatastore
