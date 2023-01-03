/*

import mongoose, { Schema } from 'mongoose'
import mongoosastic from '../lib/index'
import { MongoosasticDocument, MongoosasticModel } from '../lib/types'
import { config } from './config'
import { ITweet, Tweet } from './models/tweet'

const esClient = config.getClient()

// -- alright let's test this shiznit!
describe('updates', function () {

  beforeAll(function () {
    mongoose.connect(config.mongoUrl, config.mongoOpts, async function () {
      await config.deleteDocs([Tweet])
      await config.deleteIndexIfExists(['tweets'])
    })
  })

  afterAll(async function () {
    await config.deleteDocs([Tweet])
    await config.deleteIndexIfExists(['tweets'])

    await mongoose.disconnect()
    await esClient.close()
  })

  describe('Creating Index', function () {
    beforeAll(async function () {
      await config.createModelAndEnsureIndex(Tweet, {
        user: 'john',
        userId: 2,
        message: 'Hello folks',
        post_date: new Date()
      })
    })

    afterAll(async function () {
      await config.deleteIndexIfExists(['tweets'])
    })

    it('should be able to update', async function () {
      await Tweet.update({
        userId: 2
      }, {
        message: 'Hello world'
      })

      const mongoTweet = await Tweet.findOne({
        userId: 2
      })

      const esTweet = await Tweet.search({
        match: {
          userId:2
         }
      })

      expect(esTweet?.body.hits.hits[0]._source?.message).toEqual(mongoTweet?.message)
    })
  })
})*/
