// Claude mapper 模块
// 负责 Claude ↔ Gemini 协议转换

pub mod collector;
pub mod models;
pub mod request;
pub mod response;
pub mod streaming;
pub mod thinking_utils;
pub mod utils;

use crate::proxy::common::client_adapter::ClientAdapter;
pub use collector::collect_stream_to_json;
pub use models::*;
pub use request::{
    clean_cache_control_from_messages, merge_consecutive_messages, transform_claude_request_in,
};
pub use response::transform_response;
pub use streaming::{PartProcessor, StreamingState};
pub use thinking_utils::{
    close_tool_loop_for_thinking, filter_invalid_thinking_blocks_with_family,
}; // [NEW]

use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;

/// 创建从 Gemini SSE 流到 Claude SSE 流的转换
pub fn create_claude_sse_stream<S, E>(
    mut gemini_stream: Pin<Box<S>>,
    trace_id: String,
    email: String,
    session_id: Option<String>, // [NEW v3.3.17] Session ID for signature caching
    scaling_enabled: bool,      // [NEW] Flag for context usage scaling
    context_limit: u32,
    estimated_prompt_tokens: Option<u32>, // [FIX] Estimated tokens for calibrator learning
    message_count: usize,                 // [NEW v4.0.0] Message count for rewind detection
    client_adapter: Option<std::sync::Arc<dyn ClientAdapter>>, // [NEW] Adapter reference
    registered_tool_names: Vec<String>,   // [FIX #MCP] Tool names for fuzzy matching
) -> Pin<Box<dyn Stream<Item = Result<Bytes, String>> + Send>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + ?Sized + 'static,
    E: std::fmt::Display + Send + 'static,
{
    use async_stream::stream;
    use bytes::BytesMut;
    use futures::StreamExt;

    Box::pin(stream! {
        let mut state = StreamingState::new();
        state.session_id = session_id; // Set session ID for signature caching
        state.message_count = message_count; // [NEW v4.0.0] Set message count
        state.scaling_enabled = scaling_enabled; // Set scaling enabled flag
        state.context_limit = context_limit;
        state.estimated_prompt_tokens = estimated_prompt_tokens; // [FIX] Pass estimated tokens
        state.set_client_adapter(client_adapter); // [NEW] Set adapter
        state.set_registered_tool_names(registered_tool_names); // [FIX #MCP] Set tool names
        let mut buffer = BytesMut::new();
        let mut has_fatal_error = false;

        loop {
            // [NEW] 60秒心跳保活: 延长超时时间以增加网络抖动容错
            let next_chunk = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                gemini_stream.next()
            ).await;

            match next_chunk {
                Ok(Some(chunk_result)) => {
                    match chunk_result {
                        Ok(chunk) => {
                            buffer.extend_from_slice(&chunk);

                            // Process complete lines
                            while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                                let line_raw = buffer.split_to(pos + 1);
                                if let Ok(line_str) = std::str::from_utf8(&line_raw) {
                                    let line = line_str.trim();
                                    if line.is_empty() { continue; }

                                    if let Some(sse_chunks) = process_sse_line(line, &mut state, &trace_id, &email) {
                                        for sse_chunk in sse_chunks {
                                            yield Ok(sse_chunk);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let error_json = serde_json::json!({
                                "type": "error",
                                "error": {
                                    "type": "api_error",
                                    "message": format!("Stream error: {}", e)
                                }
                            });
                            yield Ok(Bytes::from(format!(
                                "event: error\ndata: {}\n\n",
                                error_json
                            )));
                            has_fatal_error = true;
                            break;
                        }
                    }
                }
                Ok(None) => break, // Stream 正常结束
                Err(_) => {
                    // 超时，发送心跳包 (SSE Comment 格式)
                    yield Ok(Bytes::from(": ping\n\n"));
                }
            }
        }

        if has_fatal_error {
            return;
        }

        // [FIX #1732] Mandatory Flush remaining buffer on stream termination
        // Prevents hangs when the last SSE chunk doesn't end with a newline (network fragmentation)
        if !buffer.is_empty() {
             if let Ok(line_str) = std::str::from_utf8(&buffer) {
                 let line = line_str.trim();
                 if !line.is_empty() {
                     tracing::debug!("[{}] SSE Termination: Flushing remaining {} bytes in buffer", trace_id, buffer.len());
                     if let Some(sse_chunks) = process_sse_line(line, &mut state, &trace_id, &email) {
                         for sse_chunk in sse_chunks {
                             yield Ok(sse_chunk);
                         }
                     }
                 }
             }
             buffer.clear();
        }

        // [FIX #859] Post-thinking interruption recovery
        // If we have sent thinking but NO content (text/tool_use) and the stream ended (or timed out without DONE),
        // we must provide a fallback to prevent 0-token errors on client side.
        if state.has_thinking && !state.has_content {
            tracing::warn!("[{}] Stream interrupted after thinking (No Content). Triggering recovery...", trace_id);

            // 1. Force close thinking block if open
            if state.current_block_type() == crate::proxy::mappers::claude::streaming::BlockType::Thinking {
               let close_chunks = state.end_block();
               for chunk in close_chunks {
                   yield Ok(chunk);
               }
            }

            // 2. Inject system message to inform user
            // We use a new text block for this.
            let recovery_msg = "\n\n[System] Upstream model interrupted after thinking. (Recovered by Antigravity)";
            let start_chunks = state.start_block(
                crate::proxy::mappers::claude::streaming::BlockType::Text,
                serde_json::json!({ "type": "text", "text": recovery_msg })
            );
            for chunk in start_chunks { yield Ok(chunk); }

            let stop_chunks = state.end_block();
            for chunk in stop_chunks { yield Ok(chunk); }

            // 3. Mark as content received so we don't trigger this again (though loop is done)
            state.has_content = true;

            // 4. Send a simulated usage update to ensure we have > 0 output tokens
            // Estimate based on some default if we didn't get any usage
            let recovery_usage = crate::proxy::mappers::claude::models::Usage {
                input_tokens: 0, // We don't know input, but output is critical
                output_tokens: 100, // Arbitrary small number to satisfy client
                cache_read_input_tokens: None,
                cache_creation_input_tokens: None,
                server_tool_use: None,
            };

            let delta = serde_json::json!({
                "type": "message_delta",
                "delta": { "stop_reason": "end_turn", "stop_sequence": null },
                "usage": recovery_usage
            });

            yield Ok(state.emit("message_delta", delta));
        } else if !state.has_content && !state.has_thinking {
            // [FIX #3359] Empty response recovery (e.g. single dot prompt health check)
            // If the upstream ended immediately without generating thinking or text content,
            // we must ensure message_start and at least one text block are sent before termination.
            if !state.message_start_sent {
                let dummy_start = serde_json::json!({
                    "responseId": format!("msg_recovered_{}", chrono::Utc::now().timestamp_millis()),
                    "modelVersion": "gemini-auto",
                });
                yield Ok(state.emit_message_start(&dummy_start));
            }

            let start_chunks = state.start_block(
                crate::proxy::mappers::claude::streaming::BlockType::Text,
                serde_json::json!({ "type": "text", "text": "." }),
            );
            for chunk in start_chunks {
                yield Ok(chunk);
            }
            let stop_chunks = state.end_block();
            for chunk in stop_chunks {
                yield Ok(chunk);
            }
            state.has_content = true;

            let recovery_usage = crate::proxy::mappers::claude::models::Usage {
                input_tokens: 1,
                output_tokens: 1,
                cache_read_input_tokens: None,
                cache_creation_input_tokens: None,
                server_tool_use: None,
            };
            let delta = serde_json::json!({
                "type": "message_delta",
                "delta": { "stop_reason": "end_turn", "stop_sequence": null },
                "usage": recovery_usage
            });
            yield Ok(state.emit("message_delta", delta));
        }

        // Ensure termination events are sent
        for chunk in emit_force_stop(&mut state) {
            yield Ok(chunk);
        }
    })
}

/// 处理单行 SSE 数据
fn process_sse_line(
    line: &str,
    state: &mut StreamingState,
    trace_id: &str,
    email: &str,
) -> Option<Vec<Bytes>> {
    if !line.starts_with("data: ") {
        return None;
    }

    let data_str = line[6..].trim();
    if data_str.is_empty() {
        return None;
    }

    if data_str == "[DONE]" {
        let chunks = emit_force_stop(state);
        if chunks.is_empty() {
            return None;
        }
        return Some(chunks);
    }

    // 解析 JSON
    let json_value: serde_json::Value = match serde_json::from_str(data_str) {
        Ok(v) => v,
        Err(_) => return None,
    };

    let mut chunks = Vec::new();

    // 解包 response 字段 (如果存在)
    let raw_json = json_value.get("response").unwrap_or(&json_value);

    // 发送 message_start
    if !state.message_start_sent {
        chunks.push(state.emit_message_start(raw_json));
    }

    // 捕获 groundingMetadata (Web Search)
    if let Some(candidate) = raw_json.get("candidates").and_then(|c| c.get(0)) {
        if let Some(grounding) = candidate.get("groundingMetadata") {
            // 提取搜索词
            if let Some(query) = grounding
                .get("webSearchQueries")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.get(0))
                .and_then(|v| v.as_str())
            {
                state.web_search_query = Some(query.to_string());
            }

            // 提取结果块
            if let Some(chunks_arr) = grounding.get("groundingChunks").and_then(|v| v.as_array()) {
                state.grounding_chunks = Some(chunks_arr.clone());
            } else if let Some(chunks_arr) = grounding
                .get("grounding_metadata")
                .and_then(|m| m.get("groundingChunks"))
                .and_then(|v| v.as_array())
            {
                state.grounding_chunks = Some(chunks_arr.clone());
            }
        }
    }

    // 处理所有 parts
    if let Some(parts) = raw_json
        .get("candidates")
        .and_then(|c| c.get(0))
        .and_then(|cand| cand.get("content"))
        .and_then(|content| content.get("parts"))
        .and_then(|p| p.as_array())
    {
        for part_value in parts {
            if let Ok(part) = serde_json::from_value::<GeminiPart>(part_value.clone()) {
                let mut processor = PartProcessor::new(state);
                chunks.extend(processor.process(&part));
            }
        }
    }

    // Process grounding metadata (googleSearch results) and append as citations
    // [DISABLED] Temporarily disabled to fix Cherry Studio compatibility
    // Cherry Studio doesn't recognize "web_search_tool_result" type, causing validation errors
    // Search results are still displayed via Markdown text block in streaming.rs (lines 341-381)

    /*
    if let Some(grounding) = raw_json
        .get("candidates")
        .and_then(|c| c.get(0))
        .and_then(|cand| cand.get("groundingMetadata"))
    {
        if let Some(citation_chunks) = process_grounding_metadata(grounding, state) {
            chunks.extend(citation_chunks);
        }
    }
    */

    // 检查是否结束
    if let Some(finish_reason) = raw_json
        .get("candidates")
        .and_then(|c| c.get(0))
        .and_then(|cand| cand.get("finishReason"))
        .and_then(|f| f.as_str())
    {
        let usage = raw_json
            .get("usageMetadata")
            .and_then(|u| serde_json::from_value::<UsageMetadata>(u.clone()).ok());

        if let Some(ref u) = usage {
            let cached_tokens = u.cached_content_token_count.unwrap_or(0);
            let cache_info = if cached_tokens > 0 {
                format!(", Cached: {}", cached_tokens)
            } else {
                String::new()
            };

            tracing::info!(
                "[{}] ✓ Stream completed | Account: {} | In: {} tokens | Out: {} tokens{}",
                trace_id,
                email,
                u.prompt_token_count
                    .unwrap_or(0)
                    .saturating_sub(cached_tokens),
                u.candidates_token_count.unwrap_or(0),
                cache_info
            );
        }

        chunks.extend(state.emit_finish(Some(finish_reason), usage.as_ref()));
    }

    if chunks.is_empty() {
        None
    } else {
        Some(chunks)
    }
}

/// 发送强制结束事件
pub fn emit_force_stop(state: &mut StreamingState) -> Vec<Bytes> {
    if !state.message_stop_sent {
        let mut chunks = state.emit_finish(None, None);
        if chunks.is_empty() {
            chunks.push(Bytes::from(
                "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            ));
            state.message_stop_sent = true;
        }
        return chunks;
    }
    vec![]
}

/// Process grounding metadata from Gemini's googleSearch and emit as Claude web_search blocks
#[allow(dead_code)] // Temporarily disabled for Cherry Studio compatibility, kept for future use
fn process_grounding_metadata(
    metadata: &serde_json::Value,
    state: &mut StreamingState,
) -> Option<Vec<Bytes>> {
    use serde_json::json;

    // Extract search queries and grounding chunks
    let search_queries = metadata
        .get("webSearchQueries")
        .and_then(|q| q.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
        .unwrap_or_default();

    let grounding_chunks = metadata.get("groundingChunks").and_then(|c| c.as_array())?;

    if grounding_chunks.is_empty() {
        return None;
    }

    // Generate a unique tool_use_id
    let tool_use_id = format!(
        "srvtoolu_{}",
        crate::proxy::common::utils::generate_random_id()
    );

    // Build search results array
    let mut search_results = Vec::new();
    for chunk in grounding_chunks.iter() {
        if let Some(web) = chunk.get("web") {
            let title = web
                .get("title")
                .and_then(|t| t.as_str())
                .unwrap_or("Source");
            let uri = web.get("uri").and_then(|u| u.as_str()).unwrap_or("");
            if !uri.is_empty() {
                search_results.push(json!({
                    "url": uri,
                    "title": title,
                    "encrypted_content": "", // Gemini doesn't provide this
                    "page_age": null
                }));
            }
        }
    }

    if search_results.is_empty() {
        return None;
    }

    let search_query = search_queries
        .first()
        .map(|s| s.to_string())
        .unwrap_or_default();

    tracing::debug!(
        "[Grounding] Emitting {} search results for query: {}",
        search_results.len(),
        search_query
    );

    let mut chunks = Vec::new();

    // 1. Emit server_tool_use block (start)
    let server_tool_use_start = json!({
        "type": "content_block_start",
        "index": state.block_index,
        "content_block": {
            "type": "server_tool_use",
            "id": tool_use_id,
            "name": "web_search",
            "input": {
                "query": search_query
            }
        }
    });
    chunks.push(Bytes::from(format!(
        "event: content_block_start\ndata: {}\n\n",
        server_tool_use_start
    )));

    // server_tool_use block stop
    let server_tool_use_stop = json!({
        "type": "content_block_stop",
        "index": state.block_index
    });
    chunks.push(Bytes::from(format!(
        "event: content_block_stop\ndata: {}\n\n",
        server_tool_use_stop
    )));
    state.block_index += 1;

    // 2. Emit web_search_tool_result block (start)
    let tool_result_start = json!({
        "type": "content_block_start",
        "index": state.block_index,
        "content_block": {
            "type": "web_search_tool_result",
            "tool_use_id": tool_use_id,
            "content": search_results
        }
    });
    chunks.push(Bytes::from(format!(
        "event: content_block_start\ndata: {}\n\n",
        tool_result_start
    )));

    // web_search_tool_result block stop
    let tool_result_stop = json!({
        "type": "content_block_stop",
        "index": state.block_index
    });
    chunks.push(Bytes::from(format!(
        "event: content_block_stop\ndata: {}\n\n",
        tool_result_stop
    )));
    state.block_index += 1;

    Some(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_sse_line_done() {
        let mut state = StreamingState::new();
        let result = process_sse_line("data: [DONE]", &mut state, "test_id", "test@example.com");
        assert!(result.is_some());
        let chunks = result.unwrap();
        assert!(!chunks.is_empty());

        let all_text: String = chunks
            .iter()
            .map(|b| String::from_utf8(b.to_vec()).unwrap_or_default())
            .collect();
        assert!(all_text.contains("message_stop"));
    }

    #[test]
    fn test_process_sse_line_with_text() {
        let mut state = StreamingState::new();

        let test_data = r#"data: {"candidates":[{"content":{"parts":[{"text":"Hello"}]}}],"usageMetadata":{},"modelVersion":"test","responseId":"123"}"#;

        let result = process_sse_line(test_data, &mut state, "test_id", "test@example.com");
        assert!(result.is_some());

        let chunks = result.unwrap();
        assert!(!chunks.is_empty());

        // 应该包含 message_start 和 text delta
        let all_text: String = chunks
            .iter()
            .map(|b| String::from_utf8(b.to_vec()).unwrap_or_default())
            .collect();

        assert!(all_text.contains("message_start"));
        assert!(all_text.contains("content_block_start"));
        assert!(all_text.contains("Hello"));
    }

    #[tokio::test]
    async fn test_thinking_only_interruption_recovery() {
        use futures::StreamExt;

        // 1. 模拟一个只发送 Thinking 然后就结束的流
        let mock_stream = async_stream::stream! {
            // 发送 Thinking 块
            let thinking_json = serde_json::json!({
                "candidates": [{
                    "content": {
                        "parts": [{ "text": "Thinking...", "thought": true }]
                    }
                }],
                "modelVersion": "gemini-2.0-flash-thinking",
                "responseId": "msg_interrupted"
            });
            yield Ok::<_, String>(bytes::Bytes::from(format!("data: {}\n\n", thinking_json)));

            // 然后突然结束 (没有 Text, 没有 Usage, 直接 None)
        };

        // 2. 创建转换后的流
        let mut claude_stream = create_claude_sse_stream(
            Box::pin(mock_stream),
            "trace_test".to_string(),
            "test@example.com".to_string(),
            None,
            false,
            1_000,
            None,
            1,          // message_count
            None,       // client_adapter
            Vec::new(), // registered_tool_names
        );

        // 3. 收集输出
        let mut all_chunks = Vec::new();
        while let Some(result) = claude_stream.next().await {
            if let Ok(bytes) = result {
                all_chunks.push(String::from_utf8(bytes.to_vec()).unwrap());
            }
        }
        let output = all_chunks.join("");

        // 4. 验证恢复逻辑
        // 必须包含 Thinking
        assert!(output.contains("Thinking..."));

        // 必须包含恢复的系统提示
        assert!(output.contains("Recovered by Antigravity"));

        // 必须包含模拟的 Usage
        assert!(output.contains("\"usage\":"));
        assert!(output.contains("\"output_tokens\":100")); // Should contain the recovery usage
    }

    /// 测试 A: 正常流 - 断言最后仍然包含 event: message_stop
    #[tokio::test]
    async fn test_claude_sse_stream_normal_completion() {
        use futures::StreamExt;

        let mock_stream = async_stream::stream! {
            let chunk1 = serde_json::json!({
                "candidates": [{
                    "content": {
                        "parts": [{ "text": "Hello world" }]
                    }
                }],
                "modelVersion": "gemini-2.5-pro",
                "responseId": "msg_normal_1"
            });
            yield Ok::<_, String>(bytes::Bytes::from(format!("data: {}\n\n", chunk1)));

            let chunk_done = serde_json::json!({
                "candidates": [{
                    "content": {
                        "parts": [{ "text": "" }]
                    },
                    "finishReason": "STOP"
                }],
                "usageMetadata": {
                    "promptTokenCount": 15,
                    "candidatesTokenCount": 5,
                    "totalTokenCount": 20
                },
                "modelVersion": "gemini-2.5-pro",
                "responseId": "msg_normal_2"
            });
            yield Ok::<_, String>(bytes::Bytes::from(format!("data: {}\n\n", chunk_done)));
        };

        let mut claude_stream = create_claude_sse_stream(
            Box::pin(mock_stream),
            "trace_normal".to_string(),
            "test@example.com".to_string(),
            None,
            false,
            100_000,
            None,
            1,
            None,
            Vec::new(),
        );

        let mut chunks = Vec::new();
        while let Some(result) = claude_stream.next().await {
            let bytes = result.expect("Stream chunk should be Ok");
            chunks.push(String::from_utf8(bytes.to_vec()).unwrap());
        }
        let full_output = chunks.join("");

        // 断言包含标准生命周期事件
        assert!(full_output.contains("event: message_start\n"));
        assert!(full_output.contains("event: content_block_start\n"));
        assert!(full_output.contains("event: content_block_delta\n"));
        assert!(full_output.contains("event: content_block_stop\n"));
        assert!(full_output.contains("event: message_delta\n"));
        assert!(full_output.contains("event: message_stop\n"));
        assert!(full_output.contains("data: {\"type\":\"message_stop\"}\n\n"));
    }

    /// 测试 B: 上游流读取/解析错误 - 断言包含 event: error 且 JSON 顶层 type == "error", error.type, error.message
    #[tokio::test]
    async fn test_claude_sse_stream_upstream_error_format() {
        use futures::StreamExt;

        let mock_stream = async_stream::stream! {
            yield Err::<bytes::Bytes, String>("upstream connection reset by peer".to_string());
        };

        let mut claude_stream = create_claude_sse_stream(
            Box::pin(mock_stream),
            "trace_error".to_string(),
            "test@example.com".to_string(),
            None,
            false,
            100_000,
            None,
            1,
            None,
            Vec::new(),
        );

        let mut chunks = Vec::new();
        while let Some(result) = claude_stream.next().await {
            let bytes = result.expect("Stream chunk should be Ok");
            chunks.push(String::from_utf8(bytes.to_vec()).unwrap());
        }
        let full_output = chunks.join("");

        // 断言包含标准 Anthropic SSE error event
        assert!(
            full_output.starts_with("event: error\n"),
            "Output should start with 'event: error'"
        );
        assert!(
            full_output.contains("data: "),
            "Output should contain 'data: '"
        );

        // 提取并解析 data JSON
        let data_line = full_output
            .lines()
            .find(|l| l.starts_with("data: "))
            .expect("Should have data line");
        let json_str = data_line.strip_prefix("data: ").unwrap().trim();
        let parsed: serde_json::Value =
            serde_json::from_str(json_str).expect("data must be valid JSON");

        // 验证 Anthropic error envelope 结构
        assert_eq!(parsed.get("type").and_then(|v| v.as_str()), Some("error"));
        let error_obj = parsed.get("error").expect("Must have 'error' object");
        assert_eq!(
            error_obj.get("type").and_then(|v| v.as_str()),
            Some("api_error")
        );
        let message = error_obj
            .get("message")
            .and_then(|v| v.as_str())
            .expect("Must have 'message'");
        assert!(message.contains("upstream connection reset by peer"));
    }

    /// 测试 C: fatal error 后没有 message_stop 或 message_delta
    #[tokio::test]
    async fn test_claude_sse_stream_fatal_error_no_message_stop() {
        use futures::StreamExt;

        let mock_stream = async_stream::stream! {
            let chunk1 = serde_json::json!({
                "candidates": [{
                    "content": {
                        "parts": [{ "text": "Partial text before error" }]
                    }
                }],
                "modelVersion": "gemini-2.5-pro",
                "responseId": "msg_fatal_1"
            });
            yield Ok::<_, String>(bytes::Bytes::from(format!("data: {}\n\n", chunk1)));

            // 突然发生 Fatal stream error
            yield Err::<bytes::Bytes, String>("abrupt stream disconnection".to_string());
        };

        let mut claude_stream = create_claude_sse_stream(
            Box::pin(mock_stream),
            "trace_fatal".to_string(),
            "test@example.com".to_string(),
            None,
            false,
            100_000,
            None,
            1,
            None,
            Vec::new(),
        );

        let mut chunks = Vec::new();
        while let Some(result) = claude_stream.next().await {
            let bytes = result.expect("Stream chunk should be Ok");
            chunks.push(String::from_utf8(bytes.to_vec()).unwrap());
        }
        let full_output = chunks.join("");

        // 验证输出了正常内容且之后输出了 event: error
        assert!(full_output.contains("event: message_start\n"));
        assert!(full_output.contains("Partial text before error"));
        assert!(full_output.contains("event: error\n"));

        // 验证在 event: error 之后绝不能出现 message_stop 或 message_delta
        let error_pos = full_output
            .find("event: error\n")
            .expect("Must contain event: error");
        let after_error = &full_output[error_pos..];

        assert!(
            !after_error.contains("event: message_stop"),
            "Must not emit message_stop after error"
        );
        assert!(
            !after_error.contains("event: message_delta"),
            "Must not emit message_delta after error"
        );
        assert!(
            !after_error.contains("\"type\":\"message_stop\""),
            "Must not contain message_stop JSON after error"
        );
    }

    /// 测试 D: 模拟 OMP iterateAnthropicEvents 客户端解析
    #[tokio::test]
    async fn test_claude_sse_stream_omp_client_parsing() {
        use futures::StreamExt;

        let mock_stream = async_stream::stream! {
            yield Err::<bytes::Bytes, String>("upstream timeout".to_string());
        };

        let mut claude_stream = create_claude_sse_stream(
            Box::pin(mock_stream),
            "trace_omp".to_string(),
            "test@example.com".to_string(),
            None,
            false,
            100_000,
            None,
            1,
            None,
            Vec::new(),
        );

        let mut chunks = Vec::new();
        while let Some(result) = claude_stream.next().await {
            let bytes = result.expect("Stream chunk should be Ok");
            chunks.push(String::from_utf8(bytes.to_vec()).unwrap());
        }
        let full_output = chunks.join("");

        // 模拟 OMP iterateAnthropicEvents 的逐帧解析逻辑
        let mut caught_omp_error: Option<String> = None;
        let mut saw_message_stop = false;

        // 简化的 SSE 帧提取器
        let frames: Vec<&str> = full_output
            .split("\n\n")
            .filter(|s| !s.trim().is_empty())
            .collect();
        for frame in frames {
            let mut event_name: Option<&str> = None;
            let mut data_str: Option<&str> = None;

            for line in frame.lines() {
                if let Some(ev) = line.strip_prefix("event: ") {
                    event_name = Some(ev.trim());
                } else if let Some(d) = line.strip_prefix("data: ") {
                    data_str = Some(d.trim());
                }
            }

            // OMP 核心逻辑:
            // if (sse.event === "error") { throw createAnthropicSseStreamError(sse.data); }
            if event_name == Some("error") {
                if let Some(data) = data_str {
                    let parsed: serde_json::Value = serde_json::from_str(data).unwrap();
                    let msg = parsed
                        .get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(|m| m.as_str());
                    caught_omp_error = msg.map(|s| s.to_string());
                }
            } else if event_name == Some("message_stop") {
                saw_message_stop = true;
            }
        }

        // 验证 OMP 能够识别出错误，且没有收到 message_stop
        assert!(
            caught_omp_error.is_some(),
            "OMP parser should have caught the stream error"
        );
        assert!(caught_omp_error.unwrap().contains("upstream timeout"));
        assert!(
            !saw_message_stop,
            "OMP parser should not see message_stop on stream failure"
        );
    }
}
