import { z } from 'zod/v4';
import { createJsonErrorResponseHandler } from '../../provider-utils/src';

export const openaiErrorDataSchema = z.object({
  error: z.object({
    message: z.string(),
    type: z.string().nullish(),
    param: z.any().nullish(),
    code: z.union([z.string(), z.number()]).nullish(),
  }),
});

export type OpenAIErrorData = z.infer<typeof openaiErrorDataSchema>;

export const openaiFailedResponseHandler = createJsonErrorResponseHandler({
  errorSchema: openaiErrorDataSchema,
  errorToMessage: data => data.error.message,
});
