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

interface IUser extends MongoosasticDocument {
  name: string,
  tags?: string[],
  emails?: string[],
  created_at: Date
}

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

const UserSchema = new Schema({
  id: Number,
  name: {
    type: String
  },
  tags: [{
    type: String
  }],
  emails: [{
    type: String
  }],
  created_at: {
    type: Date
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

UserSchema.plugin(mongoosastic, {
  idMapper: function (data: Record<string, unknown>) {
    return `${data.id}`
  },
  bulk: {
    size: 10, // preferred number of docs to bulk index
    delay: 100 //milliseconds to wait for enough docs to meet size constraint
  }
})

const User = mongoose.model<IUser, MongoosasticModel<IUser>>('User', UserSchema)

// -- alright let's test this shiznit!
describe('updates', function () {
  beforeAll(async function () {
    await mongoose.connect(config.mongoUrl)

    await config.deleteDocs([Message, User])
    await config.deleteIndexIfExists(['messages', 'users'])

    await config.createModelAndEnsureIndex(Tweet, {
      user: 'john',
      userId: 2,
      message: 'Hello folks',
      post_date: new Date()
    })

    await config.sleep(config.INDEXING_TIMEOUT)
  })

  afterAll(async function () {
    await config.deleteDocs([Message, User])
    await config.deleteIndexIfExists(['messages', 'users'])
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

    await MessageWithMeta.updateOne({
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

  it('addToSet', async function () {
    config.createModelAndEnsureIndex(User, {
      id: 1,
      name: 'John Doe'
    })

    await config.sleep(2000)

    await User.updateOne({
      id: 1
    }, {
      $set: {
        name: 'James Bond'
      },
      $addToSet : {
        'emails' : 'test1@test.com'
      }
    })

    await config.sleep(2000)

    await User.updateOne({
      id: 1
    }, {
      $set: {
        name: 'James Bond'
      },
      $addToSet : {
        'emails' : 'test4@test.com'
      }
    })
    

    await config.sleep(2000)

    await User.updateOne({
      id: 1
    }, {
      $set: {
        name: 'Elon Musk'
      },
      $addToSet : {
        'emails' : {
          $each : ['test1@test.com', 'test2@test.com', 'test3@test.com']
        },
        'tags' : ['a', 'b', 'c']
      }
    })

    await config.sleep(2000)

    const esUser = await User.search({
      match: {
        id : 1
      }
    })

    expect(esUser?.body.hits.hits[0]._source?.name).toEqual('Elon Musk')
    expect(esUser?.body.hits.hits[0]._source?.tags).toEqual(['a', 'b', 'c'])
    expect(esUser?.body.hits.hits[0]._source?.emails).toEqual(['test1@test.com', 'test4@test.com', 'test2@test.com', 'test3@test.com'])
  })

  it('unset', async function () {
    config.createModelAndEnsureIndex(User, {
      id: 2,
      name: 'John Doe',
      tags: ['a', 'b', 'c']
    })

    await config.sleep(2000)

    await User.updateOne({
      id: 3
    }, {
      name: 'Mr Burns',
      $unset : {
        tags : true
      }
    })

    await config.sleep(2000)

    const esUser = await User.search({
      match: {
        id : 2
      }
    })

    expect(esUser?.body.hits.hits[0]._source?.tags).not.toEqual(null)
  })

  it('upsert', async function () {
    config.createModelAndEnsureIndex(User, {
      id: 3,
      name: 'John Doe'
    })

    await config.sleep(2000)

    await User.updateOne({
      id: 4
    }, {
      $set: {
        name: 'Bart',
        tags: ['a']
      },
      $addToSet: {
        emails: 'bart@simpsons.com'
      },
      $setOnInsert: {
        created_at: new Date('Mon Jan 16 2020 14:23:47 GMT+0100 (Central European Standard Time)')
      }
    }, {
      upsert: true
    })

    await config.sleep(2000)

    const esUser = await User.search({
      match: {
        _id : 4
      }
    })

    expect(esUser?.body.hits.hits[0]._source?.name).toEqual('Bart')
    expect(esUser?.body.hits.hits[0]._source?.tags).toEqual(['a'])
    expect(esUser?.body.hits.hits[0]._source?.emails).toEqual(['bart@simpsons.com'])
    expect(esUser?.body.hits.hits[0]._source?.created_at).toEqual('2020-01-16T13:23:47.000Z')
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
  it('$set', function() {
    const generator = new ConversionGenerator()

    generator.$set({
      'a.b.c' : true,
      'd.e-f' : true,
      d : {
        e : {
          value: true
        }
      },
      f: [{
        key: 'value'
      }]
    })

    const source = generator.build()

    expect(source).toEqual({
      lang: 'painless',
      source: 'if (ctx._source[\'d\'] == null) { ctx._source[\'d\'] = [:] } if (ctx._source[\'d\'][\'e\'] == null) { ctx._source[\'d\'][\'e\'] = [:] }ctx._source[\'a.b.c\'] = params[\'a.b.c\']; ctx._source[\'d.e-f\'] = params[\'d.e-f\']; ctx._source[\'d\'][\'e\'][\'value\'] = params[\'d\'][\'e\'][\'value\']; ctx._source[\'f\'] = params[\'f\'];',
      params: {
        a: {
          b: {
            c: true
          } 
        },
        d: { 
          'e-f': true,
          e: {
            value: true
          }
        },
        f: [{
          key: 'value'
        }]
      }
    })
  }),

  it('$unset', function() {
    const generator = new ConversionGenerator()

    generator.$unset({
      'a.b.c' : true,
      'd.e_f' : true,
      d : {
        e : true
      },
      a: true
    })

    const source = generator.build()

    expect(source).toEqual({
      lang: 'painless',
      source: 'ctx._source.remove(\'a\');if ( ctx._source[\'d\'] != null ) {ctx._source[\'d\'].remove(\'e_f\');} if ( ctx._source[\'d\'] != null ) {ctx._source[\'d\'].remove(\'e\');} ',
      params: {}
    })
  })

  it('addToSet', function() {
    const generator = new ConversionGenerator()

    generator.$addToSet({
      'meta.array_string': 'new_value',
      'meta.array_string_each': {
        $each: ['a', 'b']
      },
      'meta.array_string_multiple_values': ['a', 'b'],
      'meta.array_object': {
        key1: 'value1',
        key2: 'value2'
      },
      'meta.array_object_multiple_values': [{
        key1: 'value1'
      }],
      meta2: {
        array_string_twice: 'new_value'
      },
      'meta3.test-2': {
        array_string_twice: 'new_value'
      },
      'meta4.subkey.array_object_multiple_values': [{
        key1: 'value1'
      }]
    })

    const source = generator.build()
  })
})