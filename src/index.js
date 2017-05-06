/* @flow */
'use strict'

/* :: import type {Callback, Batch, Query, QueryResult, QueryEntry} from 'interface-datastore' */

const ipfsAPI = require('ipfs-api')
const parallel = require('async/parallel')

/**
 * A datastore backed by an ipfs client accessed via the ipfs http api.
 */

class IpfsHttpApiDatastore {
  constructor (opts) {
    this.ipfs = ipfsAPI('/ip4/127.0.0.1/tcp/5001')
  }

  open (callback) {
    setTimeout(callback)
  }

  put (key /* : Key */, value /* : Buffer */, callback /* : Callback<void> */) /* : void */ {
    this.ipfs.block.put(value, key, callback)
  }

  get (key /* : Key */, callback /* : Callback<Buffer> */) /* : void */ {
    this.ipfs.block.get(key, callback)
  }

  has (key /* : Key */, callback /* : Callback<bool> */) /* : void */ {
    this.ipfs.block.get(key, (err, res) => {
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
    throw new Error('IpfsHttpApiDatastore - "query" method not supported')
  }
}

module.exports = IpfsHttpApiDatastore
