import { EventConfig, Handlers } from 'motia'
import { z } from 'zod'
import OpenAI from 'openai'

// Initialize OpenAI client. It automatically picks up process.env.OPENAI_API_KEY
const openai = new OpenAI()

export const config: EventConfig = {
  type: 'event',
  name: 'AiResponse',
  description: 'Streaming AI response handler using OpenAI',
  subscribes: ['chat-message'],
  emits: [],
  input: z.object({
    message: z.string(),
    conversationId: z.string(),
    assistantMessageId: z.string(),
  }),
}

export const handler: Handlers['AiResponse'] = async (input, { streams, logger }) => {
  const { conversationId, assistantMessageId } = input

  try {
    logger.info(`🤖 Starting AI streaming response for conversation ${conversationId}`)

    // 1. Retrieve the conversation history
    const history = await streams.conversation.getGroup(conversationId)

    // 2. Sort messages chronologically by timestamp
    const sortedHistory = history.sort(
      (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    )

    // 3. Construct history for OpenAI, including only completed messages
    const chatMessages: OpenAI.Chat.ChatCompletionMessageParam[] = sortedHistory
      .filter((msg) => msg.status === 'completed')
      .map((msg) => ({
        role: msg.from === 'user' ? 'user' : 'assistant',
        content: msg.message,
      }))

    // 4. Request streaming completion from OpenAI
    const stream = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: chatMessages,
      stream: true,
    })

    let fullResponse = ''

    // 5. Stream tokens and update the stream state in real-time
    for await (const chunk of stream) {
      const content = chunk.choices[0]?.delta?.content || ''
      if (content) {
        fullResponse += content
        await streams.conversation.set(conversationId, assistantMessageId, {
          message: fullResponse,
          from: 'assistant',
          status: 'streaming',
          timestamp: new Date().toISOString(),
        })
      }
    }

    // 6. Mark the response as completed
    await streams.conversation.set(conversationId, assistantMessageId, {
      message: fullResponse,
      from: 'assistant',
      status: 'completed',
      timestamp: new Date().toISOString(),
    })

    logger.info(`✅ AI streaming response completed for conversation ${conversationId}`)
  } catch (error: any) {
    logger.error(`❌ Error in AiResponse handler: ${error.message}`)

    // Update the assistant message in the stream to reflect the error
    try {
      await streams.conversation.set(conversationId, assistantMessageId, {
        message: `Error: Failed to generate response. ${error.message}`,
        from: 'assistant',
        status: 'completed',
        timestamp: new Date().toISOString(),
      })
    } catch (updateError: any) {
      logger.error(`❌ Failed to update error status in stream: ${updateError.message}`)
    }

    throw error
  }
}
