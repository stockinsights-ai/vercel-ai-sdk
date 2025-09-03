import { LanguageModelV2FinishReason } from '../../../provider/src';

export function mapOpenAIFinishReason(
  finishReason: string | null | undefined,
): LanguageModelV2FinishReason {
  switch (finishReason) {
    case 'stop':
      return 'stop';
    case 'length':
      return 'length';
    case 'content_filter':
      return 'content-filter';
    case 'function_call':
    case 'tool_calls':
      return 'tool-calls';
    default:
      return 'unknown';
  }
}
