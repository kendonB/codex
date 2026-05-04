use anyhow::Context;
use anyhow::Result;
use app_test_support::McpProcess;
use app_test_support::to_response;
use codex_app_server_protocol::CommittedVisibleContextItem;
use codex_app_server_protocol::CommittedVisibleContextTurn;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ItemStartedNotification;
use codex_app_server_protocol::JSONRPCNotification;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SuperplanCommitMetadata;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadSuperplanCommitParams;
use codex_app_server_protocol::ThreadSuperplanCommitResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartedNotification;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput as V2UserInput;
use codex_core::RolloutRecorder;
use codex_protocol::items::PlanItem;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InitialHistory;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutItem;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::path::Path;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[tokio::test]
async fn thread_superplan_commit_emits_replayable_plan_turn_and_is_idempotent() -> Result<()> {
    let server = responses::start_mock_server().await;
    let body = responses::sse(vec![
        responses::ev_response_created("resp-1"),
        responses::ev_assistant_message("msg-1", "Ready to implement"),
        responses::ev_completed("resp-1"),
    ]);
    let response_mock = responses::mount_sse_once(&server, body).await;

    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    let thread_req = mcp
        .send_thread_start_request(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?;
    let thread_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(thread_req)),
    )
    .await??;
    let ThreadStartResponse { thread, .. } = to_response::<ThreadStartResponse>(thread_resp)?;

    let params = superplan_commit_params(&thread.id);
    let commit_req = mcp
        .send_thread_superplan_commit_request(params.clone())
        .await?;

    let turn_started: TurnStartedNotification = read_notification(&mut mcp, "turn/started").await?;
    let item_started: ItemStartedNotification = read_notification(&mut mcp, "item/started").await?;
    let item_completed: ItemCompletedNotification =
        read_notification(&mut mcp, "item/completed").await?;
    let turn_completed: TurnCompletedNotification =
        read_notification(&mut mcp, "turn/completed").await?;
    let commit_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(commit_req)),
    )
    .await??;
    let commit = to_response::<ThreadSuperplanCommitResponse>(commit_resp)?;

    assert_eq!(turn_started.thread_id, thread.id);
    assert_eq!(turn_started.turn.status, TurnStatus::InProgress);
    assert_eq!(turn_started.turn.id, commit.final_plan_turn_id);
    assert_eq!(item_started.thread_id, thread.id);
    assert_eq!(item_started.turn_id, commit.final_plan_turn_id);
    assert_eq!(
        item_started.item,
        ThreadItem::Plan {
            id: commit.final_plan_item_id.clone(),
            text: String::new(),
        }
    );
    assert_eq!(item_completed.thread_id, thread.id);
    assert_eq!(item_completed.turn_id, commit.final_plan_turn_id);
    assert_eq!(
        item_completed.item,
        ThreadItem::Plan {
            id: commit.final_plan_item_id.clone(),
            text: FINAL_PLAN.to_string(),
        }
    );
    assert_eq!(turn_completed.thread_id, thread.id);
    assert_eq!(turn_completed.turn.status, TurnStatus::Completed);
    assert_eq!(turn_completed.turn.id, commit.final_plan_turn_id);
    assert!(!commit.already_committed);

    let duplicate_req = mcp
        .send_thread_superplan_commit_request(params.clone())
        .await?;
    let duplicate_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(duplicate_req)),
    )
    .await??;
    let duplicate = to_response::<ThreadSuperplanCommitResponse>(duplicate_resp)?;
    assert_eq!(
        duplicate,
        ThreadSuperplanCommitResponse {
            already_committed: true,
            ..commit.clone()
        }
    );

    let rollout_path = thread.path.as_ref().context("thread path missing")?;
    let history = RolloutRecorder::get_rollout_history(rollout_path).await?;
    assert_eq!(
        committed_plan_item_count(&history, &commit.final_plan_item_id),
        1
    );

    let read_req = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread.id.clone(),
            include_turns: true,
        })
        .await?;
    let read_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(read_req)),
    )
    .await??;
    let ThreadReadResponse { thread: read, .. } = to_response::<ThreadReadResponse>(read_resp)?;
    assert_eq!(read.turns.len(), 3);
    assert_user_turn(&read.turns[0].items, ORIGINAL_REQUEST);
    assert_clarification_turn(&read.turns[1].items);
    assert_eq!(
        read.turns[2].items,
        vec![ThreadItem::Plan {
            id: commit.final_plan_item_id.clone(),
            text: FINAL_PLAN.to_string(),
        }]
    );

    let turn_req = mcp
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id.clone(),
            input: vec![V2UserInput::Text {
                text: "Start implementation".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(turn_req)),
    )
    .await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;

    let model_input = response_mock.single_request().input();
    assert_text_order(
        &model_input,
        &[
            ORIGINAL_REQUEST,
            CLARIFICATION_QUESTION,
            CLARIFICATION_ANSWER,
            FINAL_PLAN,
            "Start implementation",
        ],
    );
    let final_plan_response_item = serde_json::to_value(ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: FINAL_PLAN.to_string(),
        }],
        phase: Some(MessagePhase::FinalAnswer),
    })?;
    assert!(
        model_input.contains(&final_plan_response_item),
        "final plan should be present in model-visible history"
    );

    Ok(())
}

const ORIGINAL_REQUEST: &str = "/superplan build the feature";
const CLARIFICATION_QUESTION: &str = "Superplan clarification: Which surface?";
const CLARIFICATION_ANSWER: &str = "The TUI";
const FINAL_PLAN: &str = "1. Add the command.\n2. Commit the final Plan item.";

fn superplan_commit_params(thread_id: &str) -> ThreadSuperplanCommitParams {
    ThreadSuperplanCommitParams {
        thread_id: thread_id.to_string(),
        job_id: "job-1".to_string(),
        idempotency_key: "commit-1".to_string(),
        original_user_input: Some(vec![V2UserInput::Text {
            text: ORIGINAL_REQUEST.to_string(),
            text_elements: Vec::new(),
        }]),
        committed_visible_context_turns: vec![CommittedVisibleContextTurn {
            items: vec![
                CommittedVisibleContextItem::AgentMessage {
                    text: CLARIFICATION_QUESTION.to_string(),
                },
                CommittedVisibleContextItem::UserMessage {
                    content: vec![V2UserInput::Text {
                        text: CLARIFICATION_ANSWER.to_string(),
                        text_elements: Vec::new(),
                    }],
                },
            ],
        }],
        final_plan_markdown: FINAL_PLAN.to_string(),
        metadata: SuperplanCommitMetadata {
            original_command_text: ORIGINAL_REQUEST.to_string(),
            outer_rounds_completed: 1,
            planner_q_and_a_count: 1,
        },
    }
}

async fn read_notification<T: DeserializeOwned>(mcp: &mut McpProcess, method: &str) -> Result<T> {
    let notification: JSONRPCNotification = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message(method),
    )
    .await??;
    let params = notification
        .params
        .context("expected notification params to be present")?;
    Ok(serde_json::from_value(params)?)
}

fn committed_plan_item_count(history: &InitialHistory, item_id: &str) -> usize {
    history
        .get_rollout_items()
        .iter()
        .filter(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                    item: TurnItem::Plan(PlanItem { id, .. }),
                    ..
                })) if id == item_id
            )
        })
        .count()
}

fn assert_user_turn(items: &[ThreadItem], expected_text: &str) {
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0],
        ThreadItem::UserMessage {
            id: "item-1".to_string(),
            content: vec![V2UserInput::Text {
                text: expected_text.to_string(),
                text_elements: Vec::new(),
            }],
        }
    );
}

fn assert_clarification_turn(items: &[ThreadItem]) {
    assert_eq!(items.len(), 2);
    match &items[0] {
        ThreadItem::AgentMessage { text, .. } => assert_eq!(text, CLARIFICATION_QUESTION),
        other => panic!("expected clarification agent message, got {other:?}"),
    }
    match &items[1] {
        ThreadItem::UserMessage { content, .. } => {
            assert_eq!(
                content,
                &vec![V2UserInput::Text {
                    text: CLARIFICATION_ANSWER.to_string(),
                    text_elements: Vec::new(),
                }]
            );
        }
        other => panic!("expected clarification user answer, got {other:?}"),
    }
}

fn assert_text_order(items: &[Value], needles: &[&str]) {
    let mut previous = 0usize;
    for needle in needles {
        let position = response_item_text_position(items, needle)
            .unwrap_or_else(|| panic!("model input should contain {needle:?}"));
        assert!(
            position >= previous,
            "{needle:?} should appear after earlier committed context"
        );
        previous = position;
    }
}

fn response_item_text_position(items: &[Value], needle: &str) -> Option<usize> {
    items.iter().position(|item| {
        item.get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|content| {
                content
                    .get("text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| text.contains(needle))
            })
    })
}

fn create_config_toml(codex_home: &Path, server_uri: &str) -> std::io::Result<()> {
    let config_toml = codex_home.join("config.toml");
    std::fs::write(
        config_toml,
        format!(
            r#"
model = "mock-model"
approval_policy = "never"
sandbox_mode = "read-only"

model_provider = "mock_provider"

[model_providers.mock_provider]
name = "Mock provider for test"
base_url = "{server_uri}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
"#
        ),
    )
}
