import { LanguageModelV2StreamPart } from '@ai-sdk/provider';
import { parseJsonEventStream } from '@ai-sdk/provider-utils';
import { describe, expect, it } from 'vitest';
import {
  transformOpenAIStream,
  openaiChatChunkSchema,
  openaiCompletionChunkSchema,
} from './transform-openai-stream';

async function collect(stream: AsyncIterable<LanguageModelV2StreamPart>) {
  const result: LanguageModelV2StreamPart[] = [];
  for await (const part of stream) {
    result.push(part);
  }
  return result;
}

describe('transformOpenAIStream', () => {
  it('transforms chat stream', async () => {
    const encoder = new TextEncoder();
    const sse = [
      'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-3.5-turbo-0613","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}\n\n',
      'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-3.5-turbo-0613","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}\n\n',
      'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-3.5-turbo-0613","choices":[{"index":0,"delta":{"content":", "},"finish_reason":null}]}\n\n',
      'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-3.5-turbo-0613","choices":[{"index":0,"delta":{"content":"World!"},"finish_reason":null}]}\n\n',
      'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-3.5-turbo-0613","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":17,"completion_tokens":227,"total_tokens":244}}\n\n',
      'data: [DONE]\n\n',
    ].join('');

    const byteStream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(sse));
        controller.close();
      },
    });

    const parsed = parseJsonEventStream({
      stream: byteStream,
      schema: openaiChatChunkSchema,
    });

    const stream = transformOpenAIStream(parsed, {
      responseType: 'chat',
      warnings: [],
    });

    const parts = await collect(stream);
    expect(parts).toMatchInlineSnapshot(`
      [
        {
          "type": "stream-start",
          "warnings": [],
        },
        {
          "id": "chatcmpl-1",
          "modelId": "gpt-3.5-turbo-0613",
          "timestamp": 1970-01-01T00:00:01.000Z,
          "type": "response-metadata",
        },
        {
          "id": "0",
          "type": "text-start",
        },
        {
          "delta": "",
          "id": "0",
          "type": "text-delta",
        },
        {
          "delta": "Hello",
          "id": "0",
          "type": "text-delta",
        },
        {
          "delta": ", ",
          "id": "0",
          "type": "text-delta",
        },
        {
          "delta": "World!",
          "id": "0",
          "type": "text-delta",
        },
        {
          "id": "0",
          "type": "text-end",
        },
        {
          "finishReason": "stop",
          "providerMetadata": {
            "openai": {},
          },
          "type": "finish",
          "usage": {
            "cachedInputTokens": undefined,
            "inputTokens": 17,
            "outputTokens": 227,
            "reasoningTokens": undefined,
            "totalTokens": 244,
          },
        },
      ]
    `);
  });

  it('transforms completion stream', async () => {
    const encoder = new TextEncoder();
    const sse = [
      'data: {"id":"cmpl-1","object":"text_completion.chunk","created":1,"model":"gpt-3.5-turbo-instruct","choices":[{"text":"Hello","index":0,"logprobs":null,"finish_reason":null}]}\n\n',
      'data: {"id":"cmpl-1","object":"text_completion.chunk","created":1,"model":"gpt-3.5-turbo-instruct","choices":[{"text":", ","index":0,"logprobs":null,"finish_reason":null}]}\n\n',
      'data: {"id":"cmpl-1","object":"text_completion.chunk","created":1,"model":"gpt-3.5-turbo-instruct","choices":[{"text":"World!","index":0,"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":17,"completion_tokens":227,"total_tokens":244}}\n\n',
      'data: [DONE]\n\n',
    ].join('');

    const byteStream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(sse));
        controller.close();
      },
    });

    const parsed = parseJsonEventStream({
      stream: byteStream,
      schema: openaiCompletionChunkSchema,
    });

    const stream = transformOpenAIStream(parsed, {
      responseType: 'completion',
      warnings: [],
    });

    const parts = await collect(stream);
    expect(parts).toMatchInlineSnapshot(`
      [
        {
          "type": "stream-start",
          "warnings": [],
        },
        {
          "id": "cmpl-1",
          "modelId": "gpt-3.5-turbo-instruct",
          "timestamp": 1970-01-01T00:00:01.000Z,
          "type": "response-metadata",
        },
        {
          "id": "0",
          "type": "text-start",
        },
        {
          "delta": "Hello",
          "id": "0",
          "type": "text-delta",
        },
        {
          "delta": ", ",
          "id": "0",
          "type": "text-delta",
        },
        {
          "delta": "World!",
          "id": "0",
          "type": "text-delta",
        },
        {
          "id": "0",
          "type": "text-end",
        },
        {
          "finishReason": "stop",
          "providerMetadata": {
            "openai": {},
          },
          "type": "finish",
          "usage": {
            "inputTokens": 17,
            "outputTokens": 227,
            "totalTokens": 244,
          },
        },
      ]
    `);
  });
});
