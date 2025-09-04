import {
  InvalidResponseDataError,
  LanguageModelV2CallWarning,
  LanguageModelV2FinishReason,
  LanguageModelV2StreamPart,
  LanguageModelV2Usage,
  SharedV2ProviderMetadata,
} from '../../../provider/src';
import {
  ParseResult,
  isParsableJson,
  generateId,
} from '../../../provider-utils/src';
import { z } from 'zod/v4';
import { openaiErrorDataSchema } from '../openai-error';
import { getResponseMetadata } from '../chat/get-response-metadata';
import { mapOpenAIFinishReason } from '../chat/map-openai-finish-reason';

export const openaiChatUsageSchema = z
  .object({
    prompt_tokens: z.number().nullish(),
    completion_tokens: z.number().nullish(),
    total_tokens: z.number().nullish(),
    prompt_tokens_details: z
      .object({
        cached_tokens: z.number().nullish(),
      })
      .nullish(),
    completion_tokens_details: z
      .object({
        reasoning_tokens: z.number().nullish(),
        accepted_prediction_tokens: z.number().nullish(),
        rejected_prediction_tokens: z.number().nullish(),
      })
      .nullish(),
  })
  .nullish();

export const openaiCompletionUsageSchema = z.object({
  prompt_tokens: z.number(),
  completion_tokens: z.number(),
  total_tokens: z.number(),
});

export const openaiChatChunkSchema = z.union([
  z.object({
    id: z.string().nullish(),
    created: z.number().nullish(),
    model: z.string().nullish(),
    choices: z.array(
      z.object({
        index: z.number(),
        delta: z
          .object({
            role: z.literal('assistant').nullish(),
            content: z.string().nullish(),
            tool_calls: z
              .array(
                z.object({
                  id: z.string().nullish(),
                  type: z.literal('function').nullish(),
                  index: z.number(),
                  function: z
                    .object({
                      name: z.string().nullish(),
                      arguments: z.string().nullish(),
                    })
                    .nullish(),
                }),
              )
              .nullish(),
            annotations: z
              .array(
                z.object({
                  type: z.literal('url_citation'),
                  start_index: z.number(),
                  end_index: z.number(),
                  url: z.string(),
                  title: z.string(),
                }),
              )
              .nullish(),
          })
          .nullish(),
        finish_reason: z.string().nullish(),
        logprobs: z
          .object({
            content: z
              .array(
                z.object({
                  token: z.string(),
                  logprob: z.number(),
                  top_logprobs: z.array(
                    z.object({
                      token: z.string(),
                      logprob: z.number(),
                    }),
                  ),
                }),
              )
              .nullish(),
          })
          .nullish(),
      }),
    ),
    usage: openaiChatUsageSchema,
  }),
  openaiErrorDataSchema,
]);

export const openaiCompletionChunkSchema = z.union([
  z.object({
    id: z.string().nullish(),
    created: z.number().nullish(),
    model: z.string().nullish(),
    choices: z.array(
      z.object({
        text: z.string(),
        finish_reason: z.string().nullish(),
        index: z.number(),
        logprobs: z
          .object({
            tokens: z.array(z.string()),
            token_logprobs: z.array(z.number()),
            top_logprobs: z.array(z.record(z.string(), z.number())).nullish(),
          })
          .nullish(),
      }),
    ),
    usage: openaiCompletionUsageSchema.nullish(),
  }),
  openaiErrorDataSchema,
]);

export function transformOpenAIStream(
  stream:
    | AsyncIterable<ParseResult<z.infer<typeof openaiChatChunkSchema>>>
    | ReadableStream<ParseResult<z.infer<typeof openaiChatChunkSchema>>>
    | AsyncIterable<ParseResult<z.infer<typeof openaiCompletionChunkSchema>>>
    | ReadableStream<ParseResult<z.infer<typeof openaiCompletionChunkSchema>>>,
  {
    responseType,
    warnings,
    includeRawChunks,
  }: {
    responseType: 'chat';
    warnings: Array<LanguageModelV2CallWarning>;
    includeRawChunks?: boolean;
  }): ReadableStream<LanguageModelV2StreamPart>;
export function transformOpenAIStream(
  stream:
    | AsyncIterable<ParseResult<z.infer<typeof openaiChatChunkSchema>>>
    | ReadableStream<ParseResult<z.infer<typeof openaiChatChunkSchema>>>
    | AsyncIterable<ParseResult<z.infer<typeof openaiCompletionChunkSchema>>>
    | ReadableStream<ParseResult<z.infer<typeof openaiCompletionChunkSchema>>>,
  {
    responseType,
    warnings,
    includeRawChunks,
  }: {
    responseType: 'completion';
    warnings: Array<LanguageModelV2CallWarning>;
    includeRawChunks?: boolean;
  }): ReadableStream<LanguageModelV2StreamPart>;
export function transformOpenAIStream(
  stream:
    | AsyncIterable<ParseResult<any>>
    | ReadableStream<ParseResult<any>>,
  {
    responseType,
    warnings,
    includeRawChunks,
  }: {
    responseType: 'chat' | 'completion';
    warnings: Array<LanguageModelV2CallWarning>;
    includeRawChunks?: boolean;
  }): ReadableStream<LanguageModelV2StreamPart> {
  async function* transform() {
    yield { type: 'stream-start', warnings } as LanguageModelV2StreamPart;

    let finishReason: LanguageModelV2FinishReason = 'unknown';
    const usage: LanguageModelV2Usage = {
      inputTokens: undefined,
      outputTokens: undefined,
      totalTokens: undefined,
    };
    const providerMetadata: SharedV2ProviderMetadata = { openai: {} };
    let isFirstChunk = true;

    if (responseType === 'chat') {
      const toolCalls: Array<{
        id: string;
        type: 'function';
        function: { name: string; arguments: string };
        hasFinished: boolean;
      }> = [];
      let isActiveText = false;

      for await (const chunk of stream as AsyncIterable<ParseResult<z.infer<typeof openaiChatChunkSchema>>>) {
        if (includeRawChunks) {
          yield { type: 'raw', rawValue: chunk.rawValue };
        }

        if (!chunk.success) {
          finishReason = 'error';
          yield { type: 'error', error: chunk.error };
          continue;
        }

        const value = chunk.value;

        if ('error' in value) {
          finishReason = 'error';
          yield { type: 'error', error: value.error };
          continue;
        }

        if (isFirstChunk) {
          isFirstChunk = false;
          yield {
            type: 'response-metadata',
            ...getResponseMetadata(value),
          };
        }

        if (value.usage != null) {
          usage.inputTokens = value.usage.prompt_tokens ?? undefined;
          usage.outputTokens = value.usage.completion_tokens ?? undefined;
          usage.totalTokens = value.usage.total_tokens ?? undefined;
          usage.reasoningTokens =
            value.usage.completion_tokens_details?.reasoning_tokens ?? undefined;
          usage.cachedInputTokens =
            value.usage.prompt_tokens_details?.cached_tokens ?? undefined;

          if (
            value.usage.completion_tokens_details?.accepted_prediction_tokens !=
            null
          ) {
            providerMetadata.openai.acceptedPredictionTokens =
              value.usage.completion_tokens_details?.accepted_prediction_tokens;
          }
          if (
            value.usage.completion_tokens_details?.rejected_prediction_tokens !=
            null
          ) {
            providerMetadata.openai.rejectedPredictionTokens =
              value.usage.completion_tokens_details?.rejected_prediction_tokens;
          }
        }

        const choice = value.choices[0];

        if (choice?.finish_reason != null) {
          finishReason = mapOpenAIFinishReason(choice.finish_reason);
        }

        if (choice?.logprobs?.content != null) {
          providerMetadata.openai.logprobs = choice.logprobs.content;
        }

        if (choice?.delta == null) {
          continue;
        }

        const delta = choice.delta;

        if (delta.content != null) {
          if (!isActiveText) {
            yield { type: 'text-start', id: '0' };
            isActiveText = true;
          }

          yield { type: 'text-delta', id: '0', delta: delta.content };
        }

        if (delta.tool_calls != null) {
          for (const toolCallDelta of delta.tool_calls) {
            const index = toolCallDelta.index;

            if (toolCalls[index] == null) {
              if (toolCallDelta.type !== 'function') {
                throw new InvalidResponseDataError({
                  data: toolCallDelta,
                  message: `Expected 'function' type.`,
                });
              }

              if (toolCallDelta.id == null) {
                throw new InvalidResponseDataError({
                  data: toolCallDelta,
                  message: `Expected 'id' to be a string.`,
                });
              }

              if (toolCallDelta.function?.name == null) {
                throw new InvalidResponseDataError({
                  data: toolCallDelta,
                  message: `Expected 'function.name' to be a string.`,
                });
              }

              yield {
                type: 'tool-input-start',
                id: toolCallDelta.id,
                toolName: toolCallDelta.function.name,
              };

              toolCalls[index] = {
                id: toolCallDelta.id,
                type: 'function',
                function: {
                  name: toolCallDelta.function.name,
                  arguments: toolCallDelta.function.arguments ?? '',
                },
                hasFinished: false,
              };

              const toolCall = toolCalls[index];

              if (
                toolCall.function?.name != null &&
                toolCall.function?.arguments != null
              ) {
                if (toolCall.function.arguments.length > 0) {
                  yield {
                    type: 'tool-input-delta',
                    id: toolCall.id,
                    delta: toolCall.function.arguments,
                  };
                }

                if (isParsableJson(toolCall.function.arguments)) {
                  yield { type: 'tool-input-end', id: toolCall.id };

                  yield {
                    type: 'tool-call',
                    toolCallId: toolCall.id ?? generateId(),
                    toolName: toolCall.function.name,
                    input: toolCall.function.arguments,
                  };
                  toolCall.hasFinished = true;
                }
              }

              continue;
            }

            const toolCall = toolCalls[index];

            if (toolCall.hasFinished) {
              continue;
            }

            if (toolCallDelta.function?.arguments != null) {
              toolCall.function!.arguments += toolCallDelta.function.arguments;
            }

            yield {
              type: 'tool-input-delta',
              id: toolCall.id,
              delta: toolCallDelta.function?.arguments ?? '',
            };

            if (isParsableJson(toolCall.function!.arguments)) {
              yield { type: 'tool-input-end', id: toolCall.id };

              yield {
                type: 'tool-call',
                toolCallId: toolCall.id ?? generateId(),
                toolName: toolCall.function.name,
                input: toolCall.function.arguments,
              };
              toolCall.hasFinished = true;
            }
          }
        }

        if (delta.annotations != null) {
          for (const annotation of delta.annotations) {
            yield {
              type: 'source',
              sourceType: 'url',
              id: generateId(),
              url: annotation.url,
              title: annotation.title,
            };
          }
        }
      }

      if (isActiveText) {
        yield { type: 'text-end', id: '0' };
      }

      yield {
        type: 'finish',
        finishReason,
        usage,
        ...(providerMetadata != null ? { providerMetadata } : {}),
      };
      return;
    }

    // completion response type
    for await (const chunk of stream as AsyncIterable<ParseResult<z.infer<typeof openaiCompletionChunkSchema>>>) {
      if (includeRawChunks) {
        yield { type: 'raw', rawValue: chunk.rawValue };
      }

      if (!chunk.success) {
        finishReason = 'error';
        yield { type: 'error', error: chunk.error };
        continue;
      }

      const value = chunk.value;

      if ('error' in value) {
        finishReason = 'error';
        yield { type: 'error', error: value.error };
        continue;
      }

      if (isFirstChunk) {
        isFirstChunk = false;
        yield {
          type: 'response-metadata',
          ...getResponseMetadata(value),
        };
        yield { type: 'text-start', id: '0' };
      }

      if (value.usage != null) {
        usage.inputTokens = value.usage.prompt_tokens;
        usage.outputTokens = value.usage.completion_tokens;
        usage.totalTokens = value.usage.total_tokens;
      }

      const choice = value.choices[0];

      if (choice?.finish_reason != null) {
        finishReason = mapOpenAIFinishReason(choice.finish_reason);
      }

      if (choice?.logprobs != null) {
        providerMetadata.openai.logprobs = choice.logprobs;
      }

      if (choice?.text != null && choice.text.length > 0) {
        yield { type: 'text-delta', id: '0', delta: choice.text };
      }
    }

    if (!isFirstChunk) {
      yield { type: 'text-end', id: '0' };
    }

    yield {
      type: 'finish',
      finishReason,
      providerMetadata,
      usage,
    };
  }

  return asyncIterableToReadableStream(
    transform(),
  ) as ReadableStream<LanguageModelV2StreamPart>;
}

function asyncIterableToReadableStream<T>(
  iterable: AsyncIterable<T>,
): ReadableStream<T> {
  const iterator = iterable[Symbol.asyncIterator]();
  return new ReadableStream<T>({
    async pull(controller) {
      const { value, done } = await iterator.next();
      if (done) {
        controller.close();
      } else {
        controller.enqueue(value);
      }
    },
    async cancel() {
      await iterator.return?.();
    },
  });
}

