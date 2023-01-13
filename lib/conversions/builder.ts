import { flatten, unflatten } from 'flat'
import { merge } from 'lodash'

type PainlessScript = {
  lang: 'painless';
  source: string;
  params?: Record<string, unknown>;
}

const BRACKET_NOTATION_REGEX = /\.\[/gm;
const INLINE_SCRIPT_REGEX = /\n\s{1,}/g;
const BRACKETS_SPLIT_REGEX = /\['[^[\]]*'\]/gm;

export default class ConversionGenerator {
  assertFields: string[] = []
  parameters: Record<string, unknown> = {}
  sources: string[] = []

  $set(fieldsMap: Record<string, unknown> = {}) {
    const _fields : Record<string, unknown> = flatten(fieldsMap, {
      safe: true,
      transformKey: key => `['${key}']`,
    })

    merge(this.parameters, _fields)

    const brackets = Object.keys(_fields).map(this.convertToBracketNotation)

    this.assertFields =  this.assertFields.concat(brackets)

    const source = brackets.map(bracket => `ctx._source${bracket} = params${bracket};`).join(' ')

    this.sources.push(source)
  }

  $unset(fieldsMap: Record<string, unknown> = {}) {
    const _fields : Record<string, unknown> = flatten(fieldsMap, {
      safe: true,
      transformKey: key => `['${key}']`,
    })

    merge(this.parameters, _fields)

    const brackets = Object.keys(_fields).map(this.convertToBracketNotation)

    this.assertFields =  this.assertFields.concat(brackets)

    const source = brackets.map(bracket => `ctx._source${bracket} = params${bracket};`).join(' ')

    this.sources.push(source)
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

    return this.convertMultilineScriptToInline(result);
  }
}