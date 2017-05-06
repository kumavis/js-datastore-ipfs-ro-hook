/* @flow */
'use strict'

/* :: import type {Callback, Batch, Query, QueryResult, QueryEntry} from 'interface-datastore' */

const ipfsAPI = require('ipfs-api')
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

class IpfsHttpApiDatastore {
  constructor (opts) {
    this.ipfs = ipfsAPI(opts)
  }

  open (callback) {
    setTimeout(callback)
  }

  put (key /* : Key */, value /* : Buffer */, callback /* : Callback<void> */) /* : void */ {
    const cid = dsKeyToCid(key)
    const cidString = cid.toBaseEncodedString()
    this.ipfs.block.put(value, cidString, callback)
  }

  get (key /* : Key */, callback /* : Callback<Buffer> */) /* : void */ {
    const cid = dsKeyToCid(key)
    // use cidString until this is resolved
    // https://github.com/ipfs/js-ipfs-api/pull/550
    const cidString = cid.toBaseEncodedString()
    this.ipfs.block.get(cidString, (err, block) => {
      if (err) return callback(err)
      callback(null, block.data)
    })
  }

  has (key /* : Key */, callback /* : Callback<bool> */) /* : void */ {
    const cid = dsKeyToCid(key)
    const cidString = cid.toBaseEncodedString()
    this.ipfs.block.get(cidString, (err, res) => {
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
