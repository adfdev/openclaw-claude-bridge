export interface Tool {
    name?: string;
    description?: string;
    function?: {
        name: string;
        description?: string;
        parameters?: Record<string, unknown>;
    };
}

export interface ToolCall {
    id: string;
    name: string;
    arguments: Record<string, unknown>;
}

export interface ClaudeUsage {
    input_tokens: number;
    output_tokens: number;
    cache_creation_tokens?: number;
    cache_read_tokens?: number;
    cost_usd?: number;
}

export interface ClaudeResult {
    text: string;
    usage: ClaudeUsage;
    stderrTail: string[];
}

export interface SessionEntry {
    sessionId: string;
    createdAt: number;
}

export interface ChannelEntry extends SessionEntry {
    routingKey?: string;
    lastCompactionHash?: number | null;
}

export interface LogEntry {
    id: string;
    at: string;
    model: string | null;
    tools: number;
    promptLen: number;
    inputTokens: number;
    cacheWriteTokens: number;
    cacheReadTokens: number;
    outputTokens: number;
    costUsd: number;
    durationMs: number | null;
    status: string;
    error: string | null;
    activity: string[];
    cliSessionId: string | null;
    resumed: boolean;
    channel: string | null;
    effort: string | null;
    thinking: boolean;
    resumeMethod: string | null;
    contextWindow?: number;
    agent?: string | null;
    refreshPrompt?: string;
    refreshSystemPrompt?: string;
    pendingCompactionHash?: number | null;
    usageAvailable?: boolean;
}

export interface Message {
    role: 'user' | 'assistant' | 'developer' | 'system' | 'tool';
    content: string | ContentPart[] | null;
    tool_call_id?: string;
    tool_calls?: Array<{
        id: string;
        type: string;
        function: { name: string; arguments: string };
    }>;
}

export interface ContentPart {
    type: string;
    text?: string;
    image_url?: { url: string };
}

export interface ConvertResult {
    systemPrompt: string;
    promptText: string;
}

export interface Stats {
    startedAt: Date;
    totalRequests: number;
    activeRequests: number;
    lastRequestAt: Date | null;
    lastModel: string | null;
    errors: number;
}
