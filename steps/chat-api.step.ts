import { ApiRouteConfig, Handlers } from 'motia'
import { z } from 'zod'
import * as crypto from 'crypto'

export const config: ApiRouteConfig = {
  type: 'api',
  name: 'ChatApi',
  description: 'Simple chat API endpoint',
  path: '/chat',
  method: 'POST',
  emits: ['chat-message'],
  bodySchema: z.object({
    message: z.string(),
    conversationId: z.string().optional(),
  }),
  responseSchema: {
    200: z.object({
      message: z.string(),
      from: z.enum(['user', 'assistant']),
      status: z.enum(['created', 'streaming', 'completed']),
      timestamp: z.string(),
    }),
  },
}

export const handler: Handlers['ChatApi'] = async (req, { emit, streams, logger }) => {
  try {
    const { message } = req.body
    const conversationId = req.body.conversationId || crypto.randomUUID()

    const userMessageId = crypto.randomUUID()
    const assistantMessageId = crypto.randomUUID()

    const timestamp = new Date().toISOString()

    logger.info(`💬 Received chat message for conversation ${conversationId}`)

    // Save user message in completed state
    await streams.conversation.set(conversationId, userMessageId, {
      message,
      from: 'user',
      status: 'completed',
      timestamp,
    })

    // Save initial assistant message in created state
    const assistantMessage = await streams.conversation.set(conversationId, assistantMessageId, {
      message: 'Message received, AI is responding...',
      from: 'assistant',
      status: 'created',
      timestamp,
    })

    // Emit event to start AI streaming response
    await emit({
      topic: 'chat-message',
      data: {
        message,
        conversationId,
        assistantMessageId,
      },
    })

    return {
      status: 200,
      body: {
        message: assistantMessage.message,
        from: assistantMessage.from,
        status: assistantMessage.status,
        timestamp: assistantMessage.timestamp,
      },
    }
  } catch (error: any) {
    logger.error(`❌ Error in ChatApi step handler: ${error.message}`)
    throw error
  }
}
