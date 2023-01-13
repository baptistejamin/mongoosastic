import { Client } from '@elastic/elasticsearch'
import { Query, UpdateQuery } from 'mongoose'
import { MongoosasticDocument, MongoosasticModel, Options } from './types'
import { flatten, unflatten } from 'flat'
import { bulkUpdate } from './bulking'
import { mongoConditionToQuery, shouldUsePrimaryKey } from './utils'
import ConversionGenerator from './conversions/builder'

export async function postSave(doc: MongoosasticDocument): Promise<void> {
  if (!doc) {
    return
  }

  const options = doc.esOptions()

  const filter = options && options.filter

  function onIndex(err: unknown, res: unknown) {
    if (!filter || !filter(doc)) {
      doc.emit('es-indexed', err, res)
    } else {
      doc.emit('es-filtered', err, res)
    }
  }

  const populate = options && options.populate
  if (doc) {
    if (populate && populate.length) {
      const popDoc = await doc.populate(populate)
      popDoc
        .index()
        .then((res) => onIndex(null, res))
        .catch((err) => onIndex(err, null))
    } else {
      doc
        .index()
        .then((res) => onIndex(null, res))
        .catch((err) => onIndex(err, null))
    }
  }
}

export function postRemove(doc: MongoosasticDocument): void {
  if (!doc) {
    return
  }

  doc.unIndex()
}

export function postUpdate(query: Query<unknown, unknown>, doc: MongoosasticDocument, options: Options, client: Client): void {
  const generator = new ConversionGenerator()

  const conditions = query.getFilter()

  const update =  query.getUpdate() as UpdateQuery<unknown>

  const indexName = options.index || query.model.collection.collectionName

  const $query = flatten(conditions || {})
  
  generator.$set(update.$set)
  generator.$unset(update.$unset)
  generator.$addToSet(update.$addToSet)

  const script = generator.build()

  const esQuery = mongoConditionToQuery($query as Record<string, unknown>)

  let _id =  conditions['_id']

  if (!_id && options.idMapper) {
    _id = options.idMapper(unflatten(conditions))
  }

  // $addToSet ($each) OK
  // $push : TODO
  // $unset : OK
  // test with upsert // $setOnInsert
  // $exists (with lt, gt, etc?), $ne, $or : YES

  // Extra conditions on query

  if (options.bulk && shouldUsePrimaryKey(conditions) && _id) {
    const opt = {
      index: indexName,
      id: _id,
      body: {
        script: script
      },
      bulk: options.bulk
    }

    bulkUpdate({ model: query.model as MongoosasticModel<MongoosasticDocument>, ...opt })
  } else {
    client.updateByQuery({
      index: indexName,
      body: {
        query: esQuery,
        script: script,
        conflicts: 'proceed'
      }
    })
  }
}
