use codex_app_server_protocol::CommittedVisibleContextItem;
use codex_app_server_protocol::CommittedVisibleContextTurn;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ItemStartedNotification;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadSuperplanCommitParams;
use codex_app_server_protocol::ThreadSuperplanCommitResponse;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStartedNotification;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput as ApiUserInput;
use codex_core::CodexThread;
use codex_core::RolloutRecorder;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ModeKind;
use codex_protocol::items::PlanItem;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::ItemStartedEvent;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;

pub(crate) struct SuperplanCommitOutcome {
    pub(crate) response: ThreadSuperplanCommitResponse,
    pub(crate) notifications: Vec<ServerNotification>,
}

pub(crate) enum SuperplanCommitError {
    InvalidRequest(String),
    Internal(String),
}

struct SuperplanCommitIds {
    original_request_turn_id: Option<String>,
    committed_visible_context_turn_ids: Vec<String>,
    final_plan_turn_id: String,
    final_plan_item_id: String,
}

pub(crate) async fn commit_superplan_completed_plan_turn(
    thread: &CodexThread,
    params: ThreadSuperplanCommitParams,
) -> Result<SuperplanCommitOutcome, SuperplanCommitError> {
    validate_params(&params)?;

    let thread_id = ThreadId::from_string(&params.thread_id)
        .map_err(|err| SuperplanCommitError::InvalidRequest(format!("invalid thread id: {err}")))?;
    thread.ensure_rollout_materialized().await;

    let ids = commit_ids(&params);
    if commit_is_already_persisted(thread, &ids.final_plan_item_id).await? {
        return Ok(SuperplanCommitOutcome {
            response: response_from_ids(ids, /*already_committed*/ true),
            notifications: Vec::new(),
        });
    }

    let now = chrono::Utc::now().timestamp();
    let model_items = model_visible_items(&params);
    thread
        .inject_response_items(model_items)
        .await
        .map_err(|err| {
            SuperplanCommitError::Internal(format!(
                "failed to persist model-visible Superplan context: {err}"
            ))
        })?;

    let rollout_events = rollout_events(&thread_id, &params, &ids, now);
    thread
        .append_rollout_events(&rollout_events)
        .await
        .map_err(|err| {
            SuperplanCommitError::Internal(format!(
                "failed to persist Superplan commit events: {err}"
            ))
        })?;

    Ok(SuperplanCommitOutcome {
        notifications: live_plan_notifications(
            &params.thread_id,
            &ids,
            &params.final_plan_markdown,
            now,
        ),
        response: response_from_ids(ids, /*already_committed*/ false),
    })
}

fn validate_params(params: &ThreadSuperplanCommitParams) -> Result<(), SuperplanCommitError> {
    if params.job_id.trim().is_empty() {
        return Err(SuperplanCommitError::InvalidRequest(
            "jobId must not be empty".to_string(),
        ));
    }
    if params.idempotency_key.trim().is_empty() {
        return Err(SuperplanCommitError::InvalidRequest(
            "idempotencyKey must not be empty".to_string(),
        ));
    }
    if params.final_plan_markdown.trim().is_empty() {
        return Err(SuperplanCommitError::InvalidRequest(
            "finalPlanMarkdown must not be empty".to_string(),
        ));
    }
    Ok(())
}

async fn commit_is_already_persisted(
    thread: &CodexThread,
    final_plan_item_id: &str,
) -> Result<bool, SuperplanCommitError> {
    let Some(rollout_path) = thread.rollout_path() else {
        return Ok(false);
    };
    if !rollout_path.exists() {
        return Ok(false);
    }

    let history = RolloutRecorder::get_rollout_history(&rollout_path)
        .await
        .map_err(|err| {
            SuperplanCommitError::Internal(format!("failed to read rollout history: {err}"))
        })?;
    Ok(history.scan_rollout_items(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                item: TurnItem::Plan(PlanItem { id, .. }),
                ..
            })) if id == final_plan_item_id
        )
    }))
}

fn response_from_ids(
    ids: SuperplanCommitIds,
    already_committed: bool,
) -> ThreadSuperplanCommitResponse {
    ThreadSuperplanCommitResponse {
        original_request_turn_id: ids.original_request_turn_id,
        committed_visible_context_turn_ids: ids.committed_visible_context_turn_ids,
        final_plan_turn_id: ids.final_plan_turn_id,
        final_plan_item_id: ids.final_plan_item_id,
        already_committed,
    }
}

fn commit_ids(params: &ThreadSuperplanCommitParams) -> SuperplanCommitIds {
    let hash = stable_hex_hash(&format!(
        "{}\n{}\n{}",
        params.thread_id, params.job_id, params.idempotency_key
    ));
    let prefix = format!("superplan-{hash}");
    let original_request_turn_id = params
        .original_user_input
        .as_ref()
        .map(|_| format!("{prefix}-request"));
    let committed_visible_context_turn_ids = params
        .committed_visible_context_turns
        .iter()
        .enumerate()
        .map(|(index, _)| format!("{prefix}-context-{index:03}"))
        .collect();

    SuperplanCommitIds {
        original_request_turn_id,
        committed_visible_context_turn_ids,
        final_plan_turn_id: format!("{prefix}-plan-turn"),
        final_plan_item_id: format!("{prefix}-plan-item"),
    }
}

fn stable_hex_hash(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn model_visible_items(params: &ThreadSuperplanCommitParams) -> Vec<ResponseItem> {
    let mut items = Vec::new();

    if let Some(original_user_input) = &params.original_user_input {
        items.push(user_inputs_to_response_item(original_user_input));
    }

    for turn in &params.committed_visible_context_turns {
        for item in &turn.items {
            match item {
                CommittedVisibleContextItem::UserMessage { content } => {
                    items.push(user_inputs_to_response_item(content));
                }
                CommittedVisibleContextItem::AgentMessage { text } => {
                    items.push(agent_message_response_item(text.clone(), None));
                }
            }
        }
    }

    items.push(agent_message_response_item(
        params.final_plan_markdown.clone(),
        Some(MessagePhase::FinalAnswer),
    ));
    items
}

fn user_inputs_to_response_item(inputs: &[ApiUserInput]) -> ResponseItem {
    let core_inputs = inputs
        .iter()
        .cloned()
        .map(ApiUserInput::into_core)
        .collect::<Vec<_>>();
    ResponseItem::from(ResponseInputItem::from(core_inputs))
}

fn agent_message_response_item(text: String, phase: Option<MessagePhase>) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText { text }],
        end_turn: None,
        phase,
    }
}

fn rollout_events(
    thread_id: &ThreadId,
    params: &ThreadSuperplanCommitParams,
    ids: &SuperplanCommitIds,
    now: i64,
) -> Vec<EventMsg> {
    let mut events = Vec::new();

    if let (Some(turn_id), Some(original_user_input)) = (
        ids.original_request_turn_id.as_ref(),
        params.original_user_input.as_ref(),
    ) {
        push_user_turn_events(&mut events, turn_id, original_user_input, now);
    }

    for (turn, turn_id) in params
        .committed_visible_context_turns
        .iter()
        .zip(ids.committed_visible_context_turn_ids.iter())
    {
        push_visible_context_turn_events(&mut events, turn_id, turn, now);
    }

    events.push(EventMsg::TurnStarted(turn_started(
        ids.final_plan_turn_id.clone(),
        now,
    )));
    events.push(EventMsg::ItemStarted(ItemStartedEvent {
        thread_id: *thread_id,
        turn_id: ids.final_plan_turn_id.clone(),
        item: TurnItem::Plan(PlanItem {
            id: ids.final_plan_item_id.clone(),
            text: String::new(),
        }),
    }));
    events.push(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id: *thread_id,
        turn_id: ids.final_plan_turn_id.clone(),
        item: TurnItem::Plan(PlanItem {
            id: ids.final_plan_item_id.clone(),
            text: params.final_plan_markdown.clone(),
        }),
    }));
    events.push(EventMsg::TurnComplete(turn_complete(
        ids.final_plan_turn_id.clone(),
        now,
    )));

    events
}

fn push_user_turn_events(
    events: &mut Vec<EventMsg>,
    turn_id: &str,
    user_input: &[ApiUserInput],
    now: i64,
) {
    events.push(EventMsg::TurnStarted(turn_started(
        turn_id.to_string(),
        now,
    )));
    events.push(EventMsg::UserMessage(user_message_event(user_input)));
    events.push(EventMsg::TurnComplete(turn_complete(
        turn_id.to_string(),
        now,
    )));
}

fn push_visible_context_turn_events(
    events: &mut Vec<EventMsg>,
    turn_id: &str,
    turn: &CommittedVisibleContextTurn,
    now: i64,
) {
    events.push(EventMsg::TurnStarted(turn_started(
        turn_id.to_string(),
        now,
    )));
    for item in &turn.items {
        match item {
            CommittedVisibleContextItem::UserMessage { content } => {
                events.push(EventMsg::UserMessage(user_message_event(content)));
            }
            CommittedVisibleContextItem::AgentMessage { text } => {
                events.push(EventMsg::AgentMessage(AgentMessageEvent {
                    message: text.clone(),
                    phase: None,
                    memory_citation: None,
                }));
            }
        }
    }
    events.push(EventMsg::TurnComplete(turn_complete(
        turn_id.to_string(),
        now,
    )));
}

fn turn_started(turn_id: String, now: i64) -> TurnStartedEvent {
    TurnStartedEvent {
        turn_id,
        started_at: Some(now),
        model_context_window: None,
        collaboration_mode_kind: ModeKind::Plan,
    }
}

fn turn_complete(turn_id: String, now: i64) -> TurnCompleteEvent {
    TurnCompleteEvent {
        turn_id,
        last_agent_message: None,
        completed_at: Some(now),
        duration_ms: Some(0),
    }
}

fn user_message_event(user_input: &[ApiUserInput]) -> UserMessageEvent {
    let core_inputs = user_input
        .iter()
        .cloned()
        .map(ApiUserInput::into_core)
        .collect::<Vec<_>>();

    let EventMsg::UserMessage(event) = UserMessageItem::new(&core_inputs).as_legacy_event() else {
        unreachable!("UserMessageItem always emits a legacy user message event");
    };
    event
}

fn live_plan_notifications(
    thread_id: &str,
    ids: &SuperplanCommitIds,
    plan_markdown: &str,
    now: i64,
) -> Vec<ServerNotification> {
    vec![
        ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: thread_id.to_string(),
            turn: live_turn(&ids.final_plan_turn_id, TurnStatus::InProgress, now, None),
        }),
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: thread_id.to_string(),
            turn_id: ids.final_plan_turn_id.clone(),
            item: ThreadItem::Plan {
                id: ids.final_plan_item_id.clone(),
                text: String::new(),
            },
        }),
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: thread_id.to_string(),
            turn_id: ids.final_plan_turn_id.clone(),
            item: ThreadItem::Plan {
                id: ids.final_plan_item_id.clone(),
                text: plan_markdown.to_string(),
            },
        }),
        ServerNotification::TurnCompleted(TurnCompletedNotification {
            thread_id: thread_id.to_string(),
            turn: live_turn(
                &ids.final_plan_turn_id,
                TurnStatus::Completed,
                now,
                Some(now),
            ),
        }),
    ]
}

fn live_turn(
    turn_id: &str,
    status: TurnStatus,
    started_at: i64,
    completed_at: Option<i64>,
) -> Turn {
    Turn {
        id: turn_id.to_string(),
        items: Vec::new(),
        status,
        error: None,
        started_at: Some(started_at),
        completed_at,
        duration_ms: completed_at.map(|_| 0),
    }
}
