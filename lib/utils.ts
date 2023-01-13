import { ApiResponse } from '@elastic/elasticsearch'
import { MappingProperty, PropertyName, SearchResponse, SearchTotalHits } from '@elastic/elasticsearch/api/types'
import { isEmpty } from 'lodash'
import {
  DeleteByIdOptions,
  EsSearchOptions,
  GeneratedMapping,
  HydratedSearchResults,
  MongoosasticDocument,
  MongoosasticModel,
} from './types'

import Generator from './mapping'
import painlessFields from '@shelf/es-painless-fields'

export function isString(subject: unknown): boolean {
  return typeof subject === 'string'
}

export function isStringArray(arr: Array<unknown>): boolean {
  return arr.filter && arr.length === arr.filter((item: unknown) => typeof item === 'string').length
}

export function getIndexName(doc: MongoosasticDocument | MongoosasticModel<MongoosasticDocument>): string {
  const options = doc.esOptions()
  const indexName = options && options.index
  if (!indexName) {
    return doc.collection.name
  } else {
    return indexName
  }
}

export function filterMappingFromMixed(props: Record<PropertyName, MappingProperty>): Record<PropertyName, MappingProperty> {
  const filteredMapping: Record<PropertyName, MappingProperty> = {}
  Object.keys(props).map((key) => {
    const field = props[key]
    if (field.type !== 'mixed') {
      filteredMapping[key] = field
      if (field.properties) {
        filteredMapping[key].properties = filterMappingFromMixed(field.properties)
        if (isEmpty(filteredMapping[key].properties)) {
          delete filteredMapping[key].properties
        }
      }
    }
  })
  return filteredMapping
}

export async function bodyTransform(generator: Generator, object: MongoosasticDocument) {
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  const mapping = generator.generateMapping(object.schema, true)
  const options = object.esOptions()

  let body
  if (options.customSerialize) {
    body = options.customSerialize(object, mapping)
  } else {
    body = serialize(object, mapping)
  }

  if (options.transform) {
    body = await options.transform(body, object)
  }

  return body
}

export function serialize<T extends MongoosasticDocument>(model: T, mapping: GeneratedMapping): T | T[] | string {
  let name

  function _serializeObject(object: MongoosasticDocument, mappingData: GeneratedMapping) {
    const serialized: Record<string, unknown> = {}
    let field
    let val
    for (field in mappingData.properties) {
      if (mappingData.properties?.hasOwnProperty(field)) {
        val = serialize.call(object, object[field as keyof MongoosasticDocument], mappingData.properties[field])
        if (val !== undefined) {
          serialized[field] = val
        }
      }
    }
    return serialized as T
  }

  if (mapping.properties && model) {
    if (Array.isArray(model)) {
      return model.map((object) => _serializeObject(object, mapping))
    }

    return _serializeObject(model, mapping)
  }

  const outModel = mapping.cast ? mapping.cast(model) : model
  if (typeof outModel === 'object' && outModel !== null) {
    name = outModel.constructor.name
    if (name === 'ObjectID') {
      return outModel.toString()
    }

    if (name === 'Date') {
      return new Date(outModel).toJSON()
    }
  }

  return outModel
}

export async function deleteById(document: MongoosasticDocument, opt: DeleteByIdOptions): Promise<void> {
  await opt.client
    .delete(
      {
        index: opt.index,
        id: opt.id,
      },
      {}
    )
    .then((res) => document.emit('es-removed', null, res))
    .catch((error) => document.emit('es-removed', error, null))
}

export function reformatESTotalNumber<T = unknown>(
  res: ApiResponse<SearchResponse<T>>
): ApiResponse<SearchResponse<T>> {
  Object.assign(res.body.hits, {
    total: (res.body.hits.total as SearchTotalHits).value,
    extTotal: res.body.hits.total,
  })
  return res
}

export async function hydrate(
  res: ApiResponse<SearchResponse>,
  model: MongoosasticModel<MongoosasticDocument>,
  opts: EsSearchOptions
): Promise<ApiResponse<HydratedSearchResults>> {
  const options = model.esOptions()

  const clonedRes = res as ApiResponse<HydratedSearchResults>
  const results = clonedRes.body.hits

  const resultsMap: Record<string, number> = {}

  const ids = results.hits.map((result, idx) => {
    resultsMap[result._id] = idx
    return result._id
  })

  const query = model.find({
    _id: {
      $in: ids,
    },
  })
  const hydrateOptions = opts.hydrateOptions
    ? opts.hydrateOptions
    : options.hydrateOptions
      ? options.hydrateOptions
      : {}

  // Build Mongoose query based on hydrate options
  // Example: {lean: true, sort: '-name', select: 'address name'}
  query.setOptions(hydrateOptions)

  const docs = await query.exec()

  let hits
  const docsMap: Record<string, MongoosasticDocument> = {}

  if (!docs || docs.length === 0) {
    results.hits = []
    results.hydrated = []
    clonedRes.body.hits = results
    return clonedRes
  }

  if (hydrateOptions && hydrateOptions.sort) {
    // Hydrate sort has precedence over ES result order
    hits = docs
  } else {
    // Preserve ES result ordering
    docs.forEach((doc) => {
      docsMap[doc._id] = doc
    })
    hits = results.hits.map((result) => docsMap[result._id])
  }

  if (opts.highlight || opts.hydrateWithESResults) {
    hits.forEach((doc) => {
      const idx = resultsMap[doc._id]
      if (opts.highlight) {
        doc._highlight = results.hits[idx].highlight
      }
      if (opts.hydrateWithESResults) {
        // Add to doc ES raw result (with, e.g., _score value)
        doc._esResult = results.hits[idx]
        if (!opts.hydrateWithESResults.source) {
          // Remove heavy load
          delete doc._esResult._source
        }
      }
    })
  }

  results.hits = []
  results.hydrated = hits
  clonedRes.body.hits = results

  return clonedRes
}

export function mongoSetToScript($set: Record<string, unknown>) : Record<string, unknown> {
  return painlessFields.setNotFlattened($set, true)
}

export function mongoUnsetToScript($set: Record<string, unknown>) : Record<string, unknown> {
  return painlessFields.setNotFlattened($set, true)
}

export function mongoConditionToQuery($condition: Record<string, unknown>) : object {
  const filter = []
  const must_not = []
  const bool : Record<string, unknown> = {}

  // Compare all condition items \
  // Conditions will be added in two stages :
  //   filter   : Where all positive matches will be added
  //   must_not : Where all negate matches will be added
  for (const key in $condition) {
    const currentKey = key as keyof typeof $condition
    const currentCondition = $condition[currentKey] as Record<string, unknown>

    let innerOperator = false

    if (currentCondition['$in']) {
      const terms : Record<string, unknown> = {}
      terms[currentKey] = currentCondition['$in']

      filter.push({
        terms: terms
      })

      innerOperator = true
    }

    if (currentCondition['$exists']) {
      const exists : Record<string, unknown> = {}
      exists["field"] = currentCondition['$exists']

      filter.push({
        exists: exists
      })

      innerOperator = true
    }

    if (currentCondition['$nin']) {
      const terms : Record<string, unknown> = {}
      terms[currentKey] = currentCondition['$nin']

      must_not.push({
        terms: terms
      })

      innerOperator = true
    }

    if (currentCondition['$gte'] || currentCondition['$gt'] || currentCondition['$lt']  || currentCondition['$lte']) {
      const range : Record<string, unknown> = {}

      if (currentCondition['$gte']) {
        range['gte'] = currentCondition['$gte']
      }

      if (currentCondition['$gt']) {
        range['gt'] =currentCondition['$gt']
      }

      if (currentCondition['$lte']) {
        range['lte'] = currentCondition['$lte']
      }

      if (currentCondition['$lt']) {
        range['lt'] = currentCondition['$lt']
      }

      filter.push({
        range: range
      })

      innerOperator = true
    }

    if (currentCondition['$ne']) {
      const term : Record<string, unknown> = {}
      term[currentKey] = currentCondition['$ne']

      must_not.push({
        term: term
      })

      innerOperator = true
    }

    if (currentCondition['$eq']) {
      const term : Record<string, unknown> = {}
      term[currentKey] = currentCondition['$eq']

      filter.push({
        term: term
      })

      innerOperator = true
    }

    if (innerOperator === false) {
      const term : Record<string, unknown> = {}
      term[currentKey] = currentCondition

      filter.push({
        term: term
      })
    }
  }

  if (must_not.length > 0) {
    bool['must_not'] = must_not
  }

  if (filter.length > 0) {
    bool['filter'] = filter
  }

  return {
    bool
  }
}

export function shouldUsePrimaryKey($condition: Record<string, unknown>) : boolean {
  let canUsePrimaryKey = true

  for (const key in $condition) {
    const currentKey = key as keyof typeof $condition
    const currentCondition = $condition[currentKey] as Record<string, unknown>

    if (currentCondition['$ne'] ||
        currentCondition['$nin'] ||
        currentCondition['$lte'] ||
        currentCondition['$lt'] ||
        currentCondition['$gte'] ||
        currentCondition['$gt']) {
      canUsePrimaryKey = false
    }
  }

  return canUsePrimaryKey
}