import { flatten, unflatten } from 'flat'
import { isArray, merge, set } from 'lodash'
import { v4 as uuidv4 } from 'uuid'

type PainlessScript = {
  lang: 'painless';
  source: string;
  params?: Record<string, unknown>;
}

type AddSetField = {
  stack: unknown[];
  id: string
  deduplicate: boolean,
  path: string
}

const BRACKET_NOTATION_REGEX = /\.\[/g
const INLINE_SCRIPT_REGEX = /\n\s{1,}/g
const BRACKETS_SPLIT_REGEX = /\['[^[\]]*'\]/gm

export default class ConversionGenerator {
  assertFields: string[] = []
  parameters: Record<string, unknown> = {}
  sources: string[] = []
  upsertFields: Record<string, unknown> = {}

  $set(fieldsMap: Record<string, unknown> = {}) {
    const _unflattenFields = unflatten(fieldsMap)

    const _fields : Record<string, unknown> = flatten(fieldsMap, {
      safe: true,
      transformKey: key => `['${key}']`,
    })

    // Store parameters
    merge(this.parameters, _unflattenFields)

    // Store unsertFields
    merge(this.upsertFields, _unflattenFields)

    // Use bracket notation a.b.c => a['b]['c]
    const brackets = Object.keys(_fields).map(this.convertToBracketNotation)

    this.assertFields =  this.assertFields.concat(brackets)

    const source = brackets.map(bracket => `ctx._source${bracket} = params${bracket};`).join(' ')

    // Store sources
    this.sources.push(source)
  }

  $setOnInsert(fieldsMap: Record<string, unknown> = {}) {
    const _unflattenFields = unflatten(fieldsMap)

    // Store unsertFields
    merge(this.upsertFields, _unflattenFields)
  }

  $unset(fieldsMap: Record<string, unknown> = {}) {
    const _fields : Record<string, unknown> = flatten(unflatten(fieldsMap), {
      safe: true,
      transformKey: key => `['${key}']`,
    })

    const brackets = Object.keys(_fields).map(this.convertToBracketNotation)

    let source = ''

    // Foreach key in the map
    for (const bracket of brackets) {
      // Split brackets. ["['a']", "['b']", "['c']"]
      const match = bracket.match(BRACKETS_SPLIT_REGEX)

      let assertKey = ''

      if (!match) {
        continue
      }

      let condition = ''

      // a.b.c.d -> key to delete will be d on path a.b.c
      const keyToDelete = match[match.length - 1].replace('[\'', '').replace('\']', '')

      // a && a.b && a.b.c
      for (let i = 0; i < match.length - 1; i++) {
        const currentMatch = match[i]

        if (condition != '') {
          condition += ' && '
        }

        assertKey += currentMatch
        condition += ` ctx._source${assertKey} != null `
      }

      if (keyToDelete) {
        // No condition if we are applying remove on the root level
        if (condition) {
          source += `if (${condition}) {`
        }
        
        // ctx._source['a']['b']['c'].remove('d')
        source += `ctx._source${assertKey}.remove('${keyToDelete}');`

        if (condition) {
          source += '} '
        }
      }
    }

    this.sources.push(source)
  }

  $addToSet(fieldsMap: Record<string, unknown> = {}) {
    const _fields : Record<string, unknown> = flatten(unflatten(fieldsMap), {
      safe: true,
      transformKey: key => `['${key}']`,
    })

    const addSetMap : Map<string, AddSetField> = new Map()

    const brackets = Object.keys(_fields).map(this.convertToBracketNotation)

    let source = ''

    // Foreach key in the map
    for (const bracket of brackets) {
      // Split brackets. ["['a']", "['b']", "['c']"]
      const match = bracket.match(BRACKETS_SPLIT_REGEX)

      let assertKey = ''

      if (!match) {
        continue
      }

      let condition = ''
      let currentKey = ''
      let keyToAdd = ''
      const assertFields = []

      for (let i = 0; i < match.length; i++) {
        const currentMatch = match[i]

        assertFields.push(match[i])

        assertKey += currentMatch

        // a, then a.b, then a.b.c
        if (currentKey) {
          currentKey += '.'
        }

        currentKey += currentMatch.replace('[\'', '').replace('\']', '')

        // break the pipline as soon as a current key (a.b.c) is found in the fieldsMap
        if (currentKey && fieldsMap[currentKey]) {
          keyToAdd = currentKey

          break
        }


        if (condition != '') {
          condition += ' && '
        }

        condition += ` ctx._source${assertKey} != null `
      }

      const newSet : AddSetField = {
        id: uuidv4(),
        stack: [],
        path: assertKey,
        deduplicate: true
      }
      
      if ((fieldsMap[keyToAdd] as  Record<string, unknown>).$each) {
        if (isArray((fieldsMap[keyToAdd] as  Record<string, unknown>).$each)) {
          newSet.stack = (fieldsMap[keyToAdd] as  Record<string, unknown>).$each as unknown[]
        }
      } else {
        newSet.stack = [(fieldsMap[keyToAdd] as  Record<string, unknown>)]
      }

      // set key from upsert fields
      set(this.upsertFields, keyToAdd, newSet.stack)

      addSetMap.set(keyToAdd, newSet)
  
      this.parameters[newSet.id] = newSet.stack
    }

    addSetMap.forEach(addSetField => {
      source += `
        for (value in params['${addSetField.id}']) {
          if (!ctx._source${addSetField.path}.contains(value)) {
            ctx._source${addSetField.path}.add(value)
          }
        }
      `
    })

    this.assertFields =  this.assertFields.concat(brackets)

    this.sources.push(source)
  }

  upsert () : Record<string, unknown> {
    return this.upsertFields
  }

  build() : PainlessScript {
    return {
      lang: 'painless',
      source: this.assertNullKeys(this.assertFields) + this.sources.join(' '),
      params: this.parameters,
    }
  }

  private convertToBracketNotation(key: string): string {
    return key.replace(BRACKET_NOTATION_REGEX, '[')
  }

  private convertMultilineScriptToInline(script: string): string {
    return script.replace(INLINE_SCRIPT_REGEX, ' ').trim()
  }

  private assertNullKeys(brackets: string[]): string {
    let result = ''

    for (const bracket of brackets) {
      const match = bracket.match(BRACKETS_SPLIT_REGEX)

      let assertKey = ''

      if (!match) {
        continue
      }

      for (let i = 0; i < match.length - 1; i++) {
        const currentMatch = match[i]

        assertKey += currentMatch

        result += `if (ctx._source${assertKey} == null) {
          ctx._source${assertKey} = [:]
        }
        `
      }
    }

    return this.convertMultilineScriptToInline(result)
  }
}