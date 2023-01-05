import mongoose, { Schema } from 'mongoose'
import mongoosastic from '../lib/index'
import { MongoosasticDocument, MongoosasticModel } from '../lib/types'
import { config } from './config'

interface IContact extends MongoosasticDocument {
  tenantId: string,
  email: string
}

// -- Only index specific field
const ContactSchema = new Schema({
  tenantId: {
    type: String
  },
  email: {
    type: String
  }
})

ContactSchema.plugin(mongoosastic, {
  idMapper: function (data: Record<string, unknown>) {
    return `${data.tenantId}_${data.email}`
  }
})

const Contact = mongoose.model<IContact, MongoosasticModel<IContact>>('Contact', ContactSchema)

describe('Custom ID Mode', function () {
  beforeAll(async function () {
    await config.deleteIndexIfExists(['contacts'])
    await mongoose.connect(config.mongoUrl, config.mongoOpts)
    await Contact.deleteMany()
  })

  afterAll(async function () {
    await Contact.deleteMany()
    await config.deleteIndexIfExists(['contacts'])
    await mongoose.disconnect()
  })

  it('should index with a custom ID', async function () {

    await config.createModelAndEnsureIndex(Contact, {
      tenantId: 'A',
      email: 'john.doe@gmail.com'
    })

    const results = await Contact.search({
      query_string: {
        query: 'john.doe@gmail.com'
      }
    })

    expect(results?.body.hits.hits[0]._id).toEqual('A_john.doe@gmail.com')
  })
})
