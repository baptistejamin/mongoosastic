import mongoose, { Schema } from 'mongoose'
import { config } from './config'
import mongoosastic from '../lib/index'
import { Tweet } from './models/tweet'
import { mongoConditionToQuery } from '../lib/utils'

import ConversionGenerator from '../lib/conversions/builder'

import { MongoosasticDocument, MongoosasticModel } from '../lib/types'

interface IMessage extends MongoosasticDocument {
  tenantId: string,
  messageId: string,
  content: string,
  senderId: string,
  readAt: Date
}

interface IMessageWithMeta extends MongoosasticDocument {
  tenantId: string,
  messageId: string,
  content: string,
  senderId: string,
  metadata?: {
    read: true
  }
}

// -- Only index specific field
const MessageSchema = new Schema({
  tenantId: {
    type: String
  },
  messageId: {
    type: String
  },
  content: {
    type: String
  },
  senderId: {
    type: String
  },
  readAt: {
    type: Date,
    default: null
  }
})

// -- Only index specific field
const MessageSchemaWithMeta = new Schema({
  tenantId: {
    type: String
  },
  messageId: {
    type: String
  },
  content: {
    type: String
  },
  senderId: {
    type: String
  },
  metadata: {
    read: {
      type: Boolean
    }
  }
})

MessageSchema.plugin(mongoosastic, {
  idMapper: function (data: Record<string, unknown>) {
    return `${data.tenantId}_${data.messageId}`
  },
  bulk: {
    size: 10, // preferred number of docs to bulk index
    delay: 100 //milliseconds to wait for enough docs to meet size constraint
  }
})

const Message = mongoose.model<IMessage, MongoosasticModel<IMessage>>('Message', MessageSchema)

MessageSchemaWithMeta.plugin(mongoosastic, {
  idMapper: function (data: Record<string, unknown>) {
    return `${data.tenantId}_${data.messageId}`
  },
  bulk: {
    size: 10, // preferred number of docs to bulk index
    delay: 100 //milliseconds to wait for enough docs to meet size constraint
  }
})

const MessageWithMeta = mongoose.model<IMessageWithMeta, MongoosasticModel<IMessageWithMeta>>('MessageWithMeta', MessageSchemaWithMeta)

// -- alright let's test this shiznit!
describe('updates', function () {
  beforeAll(async function () {
    await mongoose.connect(config.mongoUrl)

    await config.deleteDocs([Message])
    await config.deleteIndexIfExists(['messages'])

    await config.createModelAndEnsureIndex(Tweet, {
      user: 'john',
      userId: 2,
      message: 'Hello folks',
      post_date: new Date()
    })

    await config.sleep(config.INDEXING_TIMEOUT)
  })

  afterAll(async function () {
    await config.deleteDocs([Message])
    await config.deleteIndexIfExists(['messages'])
    await mongoose.disconnect()
  })

  it('should be able to update', async function () {
    await Tweet.updateOne({
      userId: 2
    }, {
      message: 'Hello world'
    })

    await config.sleep(config.INDEXING_TIMEOUT)

    const mongoTweet = await Tweet.findOne({
      userId: 2
    })

    const esTweet = await Tweet.search({
      match: {
        userId: 2
      }
    })

    expect(esTweet?.body.hits.hits[0]._source?.message).toEqual(mongoTweet?.message)
  })

  it('multiple updates', async function () {
    config.createModelAndEnsureIndex(Message, {
      messageId: '1',
      senderId: '1',
      tenantId: 'a',
      content: 'Hello world'
    })

    await config.sleep(config.INDEXING_TIMEOUT)

    await Message.updateMany({
      messageId: '1',
      tenantId: 'a',
    }, {
      readAt: new Date()
    })

    await config.sleep(config.INDEXING_TIMEOUT)

    const esMessage = await Message.search({
      match: {
        messageId:1
      }
    })

    expect(esMessage?.body.hits.hits[0]._source?.readAt).not.toEqual(null)
  })


  it('assert sub objects', async function () {
    config.createModelAndEnsureIndex(MessageWithMeta, {
      messageId: '2',
      senderId: '2',
      tenantId: 'b',
      content: 'Hello world'
    })

    await config.sleep(config.BULK_ACTION_TIMEOUT)

    await MessageWithMeta.update({
      messageId: '2',
      tenantId: 'b',
    }, {
      metadata : {
        read: true
      }
    })

    await config.sleep(config.BULK_ACTION_TIMEOUT)

    const esMessage = await Message.search({
      match: {
        messageId:1
      }
    })

    expect(esMessage?.body.hits.hits[0]._source?.readAt).not.toEqual(null)
  })
})

describe('mongo to elastic queries', function() {
  it('simple $in query', function() {
    const _query = mongoConditionToQuery({
      article_id : {
        $in : ['a', 'b', 'c']
      }
    })

    expect(_query).toEqual({
      bool:{
        filter:[{
          terms:{
            article_id:[
              'a','b','c'
            ]
          }
        }
        ]
      }})
  })

  it('$in with $nin', function() {
    const _query = mongoConditionToQuery({
      article_id : {
        $in : ['a', 'b', 'c'],
        $nin : ['c']
      }
    })

    expect(_query).toEqual({
      bool:{
        filter:[{
          terms:{
            article_id:[
              'a','b','c'
            ]
          }
        }],
        must_not: [{
          terms:{
            article_id:[
              'c'
            ]
          }
        }]
      }})
  })

  it('range', function() {
    const _query = mongoConditionToQuery({
      view_count : {
        $lte : 1000,
        $gte : 100
      }
    })

    expect(_query).toEqual({
      bool:{
        filter:[{
          range:{
            gte:100,
            lte:1000
          }
        }]
      }
    })
  })

  it('$ne filter', function() {
    const _query = mongoConditionToQuery({
      article_id : {
        $ne : 'd'
      }
    })

    expect(_query).toEqual({
      bool:{
        must_not:[{
          term: {
            article_id: 'd'
          }
        }]
      }
    })
  })
})


describe('mongo to elastic updates', function() {
  it('simple $set', function() {
    const _query = mongoConditionToQuery({
      article_id : {
        $in : ['a', 'b', 'c']
      }
    })

    expect(_query).toEqual({
      bool:{
        filter:[{
          terms:{
            article_id:[
              'a','b','c'
            ]
          }
        }
        ]
      }})
  })

  it('$in with $nin', function() {
    const _query = mongoConditionToQuery({
      article_id : {
        $in : ['a', 'b', 'c'],
        $nin : ['c']
      }
    })

    expect(_query).toEqual({
      bool:{
        filter:[{
          terms:{
            article_id:[
              'a','b','c'
            ]
          }
        }],
        must_not: [{
          terms:{
            article_id:[
              'c'
            ]
          }
        }]
      }})
  })

  it('range', function() {
    const _query = mongoConditionToQuery({
      view_count : {
        $lte : 1000,
        $gte : 100
      }
    })

    expect(_query).toEqual({
      bool:{
        filter:[{
          range:{
            gte:100,
            lte:1000
          }
        }]
      }
    })
  })

  it('$ne filter', function() {
    const _query = mongoConditionToQuery({
      article_id : {
        $ne : 'd'
      }
    })

    expect(_query).toEqual({
      bool:{
        must_not:[{
          term: {
            article_id: 'd'
          }
        }]
      }
    })
  })
})