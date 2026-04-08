const pollIntervalMs = 15000;
const apiBaseUrl = "/api";
const highlightDurationMs = 1800;

const state = {
  pollingTimerId: null,
  recordingsFollowupTimerId: null,
  refreshInFlight: false,
  streamers: [],
  statuses: [],
  recordings: [],
  expandedStreamerName: null,
  streamerDirectoryEntriesByName: {},
  streamerDirectorySelectionsByName: {},
  streamerDirectoryLoadingName: null,
  streamerDirectoryDeletingName: null,
  statusFilter: "active",
  recordingFilter: "all",
  recordingSort: "modified_desc",
  highlightedRecordingIds: new Set(),
  highlightResetTimerId: null,
};

const elements = {
  summaryActive: document.querySelector("#summaryActive"),
  summaryActiveMeta: document.querySelector("#summaryActiveMeta"),
  summaryRecording: document.querySelector("#summaryRecording"),
  summaryRecordingMeta: document.querySelector("#summaryRecordingMeta"),
  summaryAdBreaks: document.querySelector("#summaryAdBreaks"),
  summaryAdBreaksMeta: document.querySelector("#summaryAdBreaksMeta"),
  summaryExports: document.querySelector("#summaryExports"),
  summaryExportsMeta: document.querySelector("#summaryExportsMeta"),
  streamerForm: document.querySelector("#streamerForm"),
  streamerName: document.querySelector("#streamerName"),
  streamerCount: document.querySelector("#streamerCount"),
  streamersLabel: document.querySelector("#streamersLabel"),
  streamersList: document.querySelector("#streamersList"),
  streamersEmpty: document.querySelector("#streamersEmpty"),
  statusFilters: document.querySelector("#statusFilters"),
  focusGrid: document.querySelector("#focusGrid"),
  focusEmpty: document.querySelector("#focusEmpty"),
  recordingFilters: document.querySelector("#recordingFilters"),
  recordingSort: document.querySelector("#recordingSort"),
  recordingsTable: document.querySelector("#recordingsTable"),
  recordingsBody: document.querySelector("#recordingsTable tbody"),
  recordingsCards: document.querySelector("#recordingsCards"),
  recordingsEmpty: document.querySelector("#recordingsEmpty"),
  toast: document.querySelector("#toast"),
};

async function request(path, options = {}) {
  const response = await fetch(`${apiBaseUrl}${path}`, {
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const payload = await response.json();
      detail = payload.detail || detail;
    } catch (_) {
      // Keep the generic message when the payload is not JSON.
    }
    throw new Error(detail);
  }

  if (response.status === 204) {
    return null;
  }

  return response.json();
}

function showToast(message) {
  elements.toast.textContent = message;
  elements.toast.hidden = false;
  clearTimeout(showToast.timeoutId);
  showToast.timeoutId = window.setTimeout(() => {
    elements.toast.hidden = true;
  }, 2600);
}

function parseDate(value) {
  const timestamp = Date.parse(value || "");
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function isRecent(value, hours = 24) {
  const timestamp = parseDate(value);
  if (!timestamp) {
    return false;
  }
  return Date.now() - timestamp <= hours * 60 * 60 * 1000;
}

function formatDate(value) {
  if (!value) {
    return "N/A";
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatRelativeTime(value) {
  if (!value) {
    return "N/A";
  }
  const deltaMs = Date.now() - parseDate(value);
  if (!Number.isFinite(deltaMs) || deltaMs < 0) {
    return formatDate(value);
  }

  const totalSeconds = Math.round(deltaMs / 1000);
  if (totalSeconds < 60) {
    return `${totalSeconds}s ago`;
  }
  const totalMinutes = Math.round(totalSeconds / 60);
  if (totalMinutes < 60) {
    return `${totalMinutes}m ago`;
  }
  const totalHours = Math.round(totalMinutes / 60);
  if (totalHours < 48) {
    return `${totalHours}h ago`;
  }
  const totalDays = Math.round(totalHours / 24);
  return `${totalDays}d ago`;
}

function formatBytes(value) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB", "TB"];
  let scaled = bytes;
  let unitIndex = 0;
  while (scaled >= 1024 && unitIndex < units.length - 1) {
    scaled /= 1024;
    unitIndex += 1;
  }

  const fractionDigits = scaled >= 100 || unitIndex === 0 ? 0 : 1;
  return `${scaled.toFixed(fractionDigits)} ${units[unitIndex]}`;
}

function formatState(value) {
  if (!value) {
    return "N/A";
  }
  return String(value)
    .replace(/_/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function normalizeRecordingState(recordingState) {
  if (!recordingState) {
    return recordingState;
  }
  if (/ad[_\s-]?break/i.test(recordingState)) {
    return "recording";
  }
  return recordingState;
}

function toFileName(pathValue) {
  if (!pathValue) {
    return "N/A";
  }
  const parts = String(pathValue).split("/");
  return parts[parts.length - 1] || "N/A";
}

function hasStatusError(status) {
  const recordingState = String(status.recording_state || "").toLowerCase();
  const exitCode = Number(status.recording_exit_code);
  return Boolean(
    status.last_error
    || recordingState.includes("error")
    || recordingState.includes("failed")
    || (Number.isFinite(exitCode) && exitCode > 0 && exitCode !== 130 && recordingState !== "disabled"),
  );
}

function getLiveFocusStatuses(statuses) {
  switch (state.statusFilter) {
    case "live":
      return statuses.filter((status) => status.is_live);
    case "recording":
      return statuses.filter((status) => status.is_recording);
    case "failed":
      return statuses.filter(hasStatusError);
    case "all":
      return statuses;
    case "active":
    default:
      return statuses.filter((status) => status.is_live || status.is_recording || hasStatusError(status));
  }
}

function getRecordingError(recording) {
  return (
    recording.clean_export_error
    || recording.clean_compact_error
    || recording.clean_output_error
    || null
  );
}

function getRecordingStatus(recording) {
  if (recording.is_recording) {
    return {
      tone: "recording",
      label: "Recording",
      summary: "Capture in progress",
    };
  }

  const exportState = String(recording.clean_export_state || "none").toLowerCase();
  const compactState = String(recording.clean_compact_state || "none").toLowerCase();
  const hasError = Boolean(getRecordingError(recording));

  if (exportState === "failed" || compactState === "failed" || hasError) {
    return {
      tone: "failed",
      label: "Failed",
      summary: "Export or clean pipeline failed",
    };
  }

  if (
    exportState === "queued"
    || exportState === "processing"
    || compactState === "queued"
    || compactState === "processing"
  ) {
    return {
      tone: "processing",
      label: "Processing",
      summary: "Preparing clean output",
    };
  }

  if (exportState === "ready" || compactState === "ready") {
    return {
      tone: "ready",
      label: "Ready",
      summary: "MP4 export ready",
    };
  }

  if (recording.watchable_available || recording.artifact_mode !== "segment_native") {
    return {
      tone: "ready",
      label: "Ready",
      summary: recording.artifact_mode === "segment_native" ? "Watchable artifact ready" : "Legacy artifact ready",
    };
  }

  if (Number(recording.clean_segment_count || 0) <= 0 && recording.artifact_mode === "segment_native") {
    return {
      tone: "warning",
      label: "No Clean Content",
      summary: "No ad-free segments available",
    };
  }

  return {
    tone: "neutral",
    label: "Pending",
    summary: "Waiting for export state",
  };
}

function shouldAllowMp4Export(recording) {
  if (recording.is_recording || recording.artifact_mode !== "segment_native") {
    return false;
  }
  return Number(recording.clean_segment_count || 0) > 0;
}

function getActiveRecordingForChannel(channel) {
  return state.recordings.find((recording) => recording.channel === channel && recording.is_recording) || null;
}

function stopPollingLoop() {
  clearInterval(state.pollingTimerId);
  state.pollingTimerId = null;
}

async function runAutoRefresh() {
  if (document.hidden || state.refreshInFlight) {
    return;
  }
  try {
    await refreshAllData({ silent: true });
  } catch (error) {
    showToast(error.message);
  }
}

function startPollingLoop() {
  stopPollingLoop();
  state.pollingTimerId = window.setInterval(async () => {
    if (document.hidden) {
      return;
    }
    await runAutoRefresh();
  }, pollIntervalMs);
}

function scheduleRecordingsFollowupRefresh() {
  if (state.recordingsFollowupTimerId !== null) {
    return;
  }
  state.recordingsFollowupTimerId = window.setTimeout(async () => {
    state.recordingsFollowupTimerId = null;
    try {
      await refreshRecordings();
    } catch (_) {
      // Best-effort background refresh for export status transitions.
    }
  }, 2000);
}

function clearRecordingsFollowupRefresh() {
  if (state.recordingsFollowupTimerId !== null) {
    window.clearTimeout(state.recordingsFollowupTimerId);
    state.recordingsFollowupTimerId = null;
  }
}

function clearRecordingHighlightsLater() {
  clearTimeout(state.highlightResetTimerId);
  state.highlightResetTimerId = window.setTimeout(() => {
    state.highlightedRecordingIds.clear();
    renderRecordings();
  }, highlightDurationMs);
}

function markUpdatedRecordings(previousRecordings, nextRecordings) {
  const previousById = new Map(previousRecordings.map((recording) => [recording.recording_id, recording]));
  const changedIds = new Set();

  for (const recording of nextRecordings) {
    const previous = previousById.get(recording.recording_id);
    if (!previous) {
      changedIds.add(recording.recording_id);
      continue;
    }
    if (
      previous.modified_at !== recording.modified_at
      || previous.is_recording !== recording.is_recording
      || previous.clean_export_state !== recording.clean_export_state
      || previous.clean_compact_state !== recording.clean_compact_state
      || getRecordingError(previous) !== getRecordingError(recording)
    ) {
      changedIds.add(recording.recording_id);
    }
  }

  state.highlightedRecordingIds = changedIds;
  if (changedIds.size) {
    clearRecordingHighlightsLater();
  }
}

function createAvatar(name, profileImageUrl) {
  if (profileImageUrl) {
    const image = document.createElement("img");
    image.className = "channel-avatar";
    image.src = profileImageUrl;
    image.alt = `${name} avatar`;
    image.loading = "lazy";
    return image;
  }

  const fallback = document.createElement("div");
  fallback.className = "channel-avatar channel-avatar-fallback";
  fallback.textContent = (name || "?").slice(0, 1).toUpperCase();
  fallback.setAttribute("aria-hidden", "true");
  return fallback;
}

function createStatusBadge(tone, text) {
  const badge = document.createElement("span");
  badge.className = `status-badge status-${tone}`;
  badge.textContent = text;
  return badge;
}

function createStatBlock(label, value) {
  const wrapper = document.createElement("div");
  wrapper.className = "focus-stat";

  const labelNode = document.createElement("span");
  labelNode.className = "focus-stat-label";
  labelNode.textContent = label;

  const valueNode = document.createElement("span");
  valueNode.className = "focus-stat-value";
  valueNode.textContent = value;

  wrapper.append(labelNode, valueNode);
  return wrapper;
}

function renderSummaryCards() {
  const activeCount = state.statuses.filter((status) => status.is_live || status.is_recording).length;
  const recordingCount = state.statuses.filter((status) => status.is_recording).length;
  const liveCount = state.statuses.filter((status) => status.is_live).length;
  const monitoredCount = state.streamers.length;
  const recentRecordings = state.recordings.filter((recording) => isRecent(recording.modified_at, 24));
  const adBreakCount = recentRecordings.reduce((total, recording) => total + Number(recording.ad_break_count || 0), 0);
  const readyExports = recentRecordings.filter((recording) => (
    String(recording.clean_export_state || "none").toLowerCase() === "ready"
    || String(recording.clean_compact_state || "none").toLowerCase() === "ready"
  )).length;
  const pendingExports = recentRecordings.filter((recording) => (
    ["queued", "processing"].includes(String(recording.clean_export_state || "none").toLowerCase())
    || ["queued", "processing"].includes(String(recording.clean_compact_state || "none").toLowerCase())
  )).length;

  elements.summaryActive.textContent = String(activeCount);
  elements.summaryActiveMeta.textContent = `${liveCount} live • ${monitoredCount} monitored`;

  elements.summaryRecording.textContent = String(recordingCount);
  elements.summaryRecordingMeta.textContent = recordingCount
    ? `${recordingCount} capture session${recordingCount === 1 ? "" : "s"} active`
    : "No active recordings";

  elements.summaryAdBreaks.textContent = String(adBreakCount);
  elements.summaryAdBreaksMeta.textContent = recentRecordings.length
    ? `${recentRecordings.length} session${recentRecordings.length === 1 ? "" : "s"} updated in 24h`
    : "No recent sessions";

  elements.summaryExports.textContent = String(readyExports);
  elements.summaryExportsMeta.textContent = pendingExports
    ? `${pendingExports} export${pendingExports === 1 ? "" : "s"} still processing`
    : "Ready in the last 24 hours";

  elements.streamerCount.textContent = `${monitoredCount} monitored`;
  elements.streamersLabel.textContent = monitoredCount
    ? `${liveCount} live • ${recordingCount} recording`
    : "Manage recording policy";
}

async function createCleanExport(recording, mode = "retry") {
  try {
    const query = new URLSearchParams({ mode }).toString();
    await request(`/recordings/${encodeURIComponent(recording.recording_id)}/exports/clean-mp4?${query}`, {
      method: "POST",
    });
    showToast(`${mode === "force" ? "Forced" : "Queued"} MP4 export for ${recording.channel}`);
    await refreshRecordings();
  } catch (error) {
    showToast(error.message);
  }
}

function createRecordingActions(recording) {
  const actions = document.createElement("div");
  actions.className = "recording-actions";

  const recordingStatus = getRecordingStatus(recording);
  if (recordingStatus.tone === "ready" && recording.clean_export_state === "ready") {
    const mp4Download = document.createElement("a");
    mp4Download.className = "action-link small";
    mp4Download.href = `${apiBaseUrl}/recordings/${encodeURIComponent(recording.recording_id)}/download/clean-mp4`;
    mp4Download.textContent = "MP4";
    actions.append(mp4Download);
  } else if (shouldAllowMp4Export(recording)) {
    const exportButton = document.createElement("button");
    exportButton.className = "secondary small";
    exportButton.type = "button";
    exportButton.textContent = recordingStatus.tone === "failed" ? "Retry MP4" : "Prepare MP4";
    exportButton.addEventListener("click", async () => {
      exportButton.disabled = true;
      exportButton.textContent = "Queueing...";
      await createCleanExport(recording, recordingStatus.tone === "failed" ? "force" : "retry");
      exportButton.disabled = false;
      exportButton.textContent = recordingStatus.tone === "failed" ? "Retry MP4" : "Prepare MP4";
    });
    actions.append(exportButton);
  }

  return actions;
}

function createRecordingErrorDetails(recording) {
  const errorText = getRecordingError(recording);
  if (!errorText) {
    return null;
  }

  const details = document.createElement("details");
  details.className = "details-toggle";

  const summary = document.createElement("summary");
  summary.textContent = "Show error details";

  const content = document.createElement("p");
  content.textContent = errorText;

  details.append(summary, content);
  return details;
}

function createFocusCard(status) {
  const activeRecording = getActiveRecordingForChannel(status.name);
  const recordingStartValue = status.is_recording ? formatDate(status.recording_started_at) : "Not recording";
  const artifactModeValue = activeRecording ? formatState(activeRecording.artifact_mode) : (status.is_recording ? "Unknown" : "Idle");
  const outputValue = status.is_recording
    ? (activeRecording?.clean_export_dir_path || status.output_path || "N/A")
    : "Not recording";
  const card = document.createElement("article");
  card.className = "focus-card";
  if (status.is_recording) {
    card.classList.add("is-recording");
  }
  if (hasStatusError(status)) {
    card.classList.add("is-failed");
  }

  const top = document.createElement("div");
  top.className = "focus-top";

  const identity = document.createElement("div");
  identity.className = "focus-identity";
  identity.append(createAvatar(status.name, status.profile_image_url));

  const nameWrap = document.createElement("div");
  nameWrap.className = "focus-name-wrap";

  const nameLine = document.createElement("div");
  nameLine.className = "focus-name-line";

  const name = document.createElement("strong");
  name.className = "focus-name";
  name.textContent = status.name;
  nameLine.append(name);

  const subtitle = document.createElement("p");
  subtitle.className = "focus-subtitle";
  subtitle.textContent = status.game_name || "No category";

  const title = document.createElement("p");
  title.className = "focus-title";
  title.textContent = status.title || "No live title available";

  const badges = document.createElement("div");
  badges.className = "focus-badges";
  badges.append(createStatusBadge(status.is_live ? "live" : "offline", status.is_live ? "LIVE" : "OFFLINE"));

  if (status.is_recording) {
    badges.append(createStatusBadge("recording", "RECORDING"));
  }
  if (status.stop_after_at && !status.is_live && status.is_recording) {
    badges.append(createStatusBadge("warning", "STOPPING"));
  }
  if (hasStatusError(status)) {
    badges.append(createStatusBadge("failed", "ERROR"));
  }

  nameWrap.append(nameLine, title, subtitle);
  if (status.is_recording) {
    const pulse = document.createElement("div");
    pulse.className = "recording-pulse";
    const dot = document.createElement("span");
    dot.className = "recording-dot";
    pulse.append(dot, document.createTextNode("Recording in progress"));
    nameWrap.append(pulse);
  }

  identity.append(nameWrap);
  top.append(identity, badges);

  const stats = document.createElement("div");
  stats.className = "focus-stats";
  stats.append(
    createStatBlock("Viewers", String(status.viewer_count ?? "N/A")),
    createStatBlock("Recording State", formatState(normalizeRecordingState(status.recording_state))),
    createStatBlock("Recording Started", recordingStartValue),
    createStatBlock("Artifact Mode", artifactModeValue),
    createStatBlock("Checked", formatDate(status.last_checked_at)),
    createStatBlock("Output", outputValue),
  );

  const actions = document.createElement("div");
  actions.className = "focus-actions";

  if (status.is_recording) {
    const stopButton = document.createElement("button");
    stopButton.className = "danger";
    stopButton.type = "button";
    stopButton.textContent = "Stop Recording";
    stopButton.addEventListener("click", async () => {
      stopButton.disabled = true;
      stopButton.textContent = "Stopping...";
      try {
        const result = await request(`/streamers/${encodeURIComponent(status.name)}/stop`, {
          method: "POST",
        });
        showToast(result.stopped ? `Stopped recording for ${status.name}` : `No active recording for ${status.name}`);
        await refreshAllData();
      } catch (error) {
        stopButton.disabled = false;
        stopButton.textContent = "Stop Recording";
        showToast(error.message);
      }
    });
    actions.append(stopButton);
  } else if (status.is_live) {
    if (status.enabled_for_recording === false) {
      actions.append(createStatusBadge("warning", "Recording Disabled"));
    } else {
      const startButton = document.createElement("button");
      startButton.type = "button";
      startButton.textContent = "Start Recording";
      startButton.addEventListener("click", async () => {
        startButton.disabled = true;
        startButton.textContent = "Starting...";
        try {
          const result = await request(`/streamers/${encodeURIComponent(status.name)}/start`, {
            method: "POST",
          });
          showToast(result.started ? `Started recording for ${status.name}` : `Could not start recording for ${status.name}`);
          await refreshAllData();
        } catch (error) {
          startButton.disabled = false;
          startButton.textContent = "Start Recording";
          showToast(error.message);
        }
      });
      actions.append(startButton);
    }
  }

  card.append(top, stats, actions);

  if (status.last_error) {
    const errorDetails = document.createElement("details");
    errorDetails.className = "details-toggle";
    const summary = document.createElement("summary");
    summary.textContent = "Show last error";
    const text = document.createElement("p");
    text.textContent = status.last_error;
    errorDetails.append(summary, text);
    card.append(errorDetails);
  }

  return card;
}

function renderStatuses() {
  const statuses = getLiveFocusStatuses(state.statuses);
  elements.focusGrid.replaceChildren();
  elements.focusEmpty.hidden = statuses.length > 0;

  for (const button of elements.statusFilters.querySelectorAll("[data-filter]")) {
    button.classList.toggle("is-active", button.dataset.filter === state.statusFilter);
  }

  if (!statuses.length) {
    return;
  }

  for (const status of statuses) {
    elements.focusGrid.append(createFocusCard(status));
  }
}

function getStreamerStatus(streamerName) {
  return state.statuses.find((status) => status.name === streamerName) || null;
}

function getStreamerDirectorySelections(name) {
  return state.streamerDirectorySelectionsByName[name] || [];
}

function setStreamerDirectorySelection(name, recordingId, selected) {
  const current = new Set(getStreamerDirectorySelections(name));
  if (selected) {
    current.add(recordingId);
  } else {
    current.delete(recordingId);
  }
  state.streamerDirectorySelectionsByName[name] = Array.from(current);
}

async function loadStreamerRecordingDirectories(name) {
  state.streamerDirectoryLoadingName = name;
  renderStreamers();
  try {
    const directories = await request(`/streamers/${encodeURIComponent(name)}/recording-directories`);
    state.streamerDirectoryEntriesByName[name] = directories;
  } finally {
    state.streamerDirectoryLoadingName = null;
  }
}

async function toggleStreamerDirectoryPanel(name) {
  if (state.expandedStreamerName === name) {
    state.expandedStreamerName = null;
    delete state.streamerDirectorySelectionsByName[name];
    renderStreamers();
    return;
  }

  state.expandedStreamerName = name;
  delete state.streamerDirectorySelectionsByName[name];
  renderStreamers();

  try {
    await loadStreamerRecordingDirectories(name);
    renderStreamers();
  } catch (error) {
    state.expandedStreamerName = null;
    renderStreamers();
    showToast(error.message);
  }
}

async function deleteStreamerRecordingDirectories(name) {
  const selectedIds = getStreamerDirectorySelections(name);
  if (!selectedIds.length) {
    return;
  }

  const confirmed = window.confirm(
    `Delete ${selectedIds.length} recording director${selectedIds.length === 1 ? "y" : "ies"} for ${name}?`,
  );
  if (!confirmed) {
    return;
  }

  state.streamerDirectoryDeletingName = name;
  renderStreamers();
  try {
    const result = await request(`/streamers/${encodeURIComponent(name)}/recording-directories/delete`, {
      method: "POST",
      body: JSON.stringify({ recording_ids: selectedIds }),
    });
    delete state.streamerDirectorySelectionsByName[name];
    delete state.streamerDirectoryEntriesByName[name];
    showToast(`Deleted ${result.deleted_recording_ids.length} recording directories for ${name}`);
    await Promise.all([refreshStreamers(), refreshRecordings()]);
    if (state.expandedStreamerName === name) {
      await loadStreamerRecordingDirectories(name);
      renderStreamers();
    }
  } catch (error) {
    showToast(error.message);
  } finally {
    state.streamerDirectoryDeletingName = null;
    renderStreamers();
  }
}

async function setStreamerRecordingEnabled(name, enabledForRecording) {
  const updated = await request(`/streamers/${encodeURIComponent(name)}`, {
    method: "PATCH",
    body: JSON.stringify({ enabled_for_recording: enabledForRecording }),
  });
  state.streamers = state.streamers.map((streamer) => (
    streamer.name === updated.name ? updated : streamer
  ));
  renderStreamers();
  renderSummaryCards();
  await Promise.all([refreshStatuses(), refreshRecordings()]);
  showToast(`${updated.name} recording ${updated.enabled_for_recording ? "enabled" : "disabled"}`);
}

function renderStreamers() {
  elements.streamersList.replaceChildren();
  elements.streamersEmpty.hidden = state.streamers.length > 0;

  if (state.expandedStreamerName && !state.streamers.some((streamer) => streamer.name === state.expandedStreamerName)) {
    state.expandedStreamerName = null;
  }

  for (const streamer of state.streamers) {
    const status = getStreamerStatus(streamer.name);
    const item = document.createElement("li");
    item.className = "streamer-item";

    const main = document.createElement("div");
    main.className = "streamer-item-main";

    const top = document.createElement("div");
    top.className = "streamer-item-top";

    const left = document.createElement("div");

    const nameButton = document.createElement("button");
    nameButton.className = "streamer-name-button";
    nameButton.type = "button";
    nameButton.textContent = streamer.name;
    nameButton.setAttribute("aria-expanded", String(state.expandedStreamerName === streamer.name));
    nameButton.addEventListener("click", async () => {
      await toggleStreamerDirectoryPanel(streamer.name);
    });

    const badgeRow = document.createElement("div");
    badgeRow.className = "status-stack";
    badgeRow.append(createStatusBadge(
      status?.is_live ? "live" : "offline",
      status?.is_live ? "LIVE" : "OFFLINE",
    ));
    if (status?.is_recording) {
      badgeRow.append(createStatusBadge("recording", "RECORDING"));
    }
    if (hasStatusError(status || {})) {
      badgeRow.append(createStatusBadge("failed", "ERROR"));
    }

    left.append(nameButton, badgeRow);

    const controls = document.createElement("div");
    controls.className = "streamer-item-controls";

    const toggle = document.createElement("label");
    toggle.className = "toggle";

    const toggleInput = document.createElement("input");
    toggleInput.type = "checkbox";
    toggleInput.checked = streamer.enabled_for_recording !== false;
    toggleInput.addEventListener("change", async () => {
      const nextValue = toggleInput.checked;
      toggleInput.disabled = true;
      try {
        await setStreamerRecordingEnabled(streamer.name, nextValue);
      } catch (error) {
        toggleInput.checked = !nextValue;
        showToast(error.message);
      } finally {
        toggleInput.disabled = false;
      }
    });

    const toggleText = document.createElement("span");
    toggleText.textContent = "Auto Record";
    toggle.append(toggleInput, toggleText);

    const removeButton = document.createElement("button");
    removeButton.className = "ghost";
    removeButton.type = "button";
    removeButton.textContent = "Remove";
    removeButton.addEventListener("click", async () => {
      try {
        await request(`/streamers/${encodeURIComponent(streamer.name)}`, { method: "DELETE" });
        showToast(`Removed ${streamer.name}`);
        await refreshAllData();
      } catch (error) {
        showToast(error.message);
      }
    });

    controls.append(toggle, removeButton);
    top.append(left, controls);
    main.append(top);

    if (state.expandedStreamerName === streamer.name) {
      const directories = document.createElement("div");
      directories.className = "streamer-directories";

      const shell = document.createElement("div");
      shell.className = "directory-shell";

      if (state.streamerDirectoryLoadingName === streamer.name) {
        const loading = document.createElement("div");
        loading.className = "recording-muted";
        loading.textContent = "Loading recording directories...";
        shell.append(loading);
      } else {
        const directoryItems = state.streamerDirectoryEntriesByName[streamer.name] || [];
        if (!directoryItems.length) {
          const empty = document.createElement("div");
          empty.className = "recording-muted";
          empty.textContent = "No deletable recordings for this streamer.";
          shell.append(empty);
        } else {
          const list = document.createElement("div");
          list.className = "streamer-directory-list";

          for (const directory of directoryItems) {
            const entry = document.createElement("label");
            entry.className = "streamer-directory-item";

            const checkbox = document.createElement("input");
            checkbox.type = "checkbox";
            checkbox.checked = getStreamerDirectorySelections(streamer.name).includes(directory.recording_id);
            checkbox.addEventListener("change", () => {
              setStreamerDirectorySelection(streamer.name, directory.recording_id, checkbox.checked);
              renderStreamers();
            });

            const copy = document.createElement("div");
            const directoryName = document.createElement("div");
            directoryName.className = "directory-name";
            directoryName.textContent = directory.directory_name;

            const directoryMeta = document.createElement("div");
            directoryMeta.className = "directory-meta";
            directoryMeta.textContent = `Ended ${formatDate(directory.ended_at || directory.modified_at)}`;

            copy.append(directoryName, directoryMeta);
            entry.append(checkbox, copy);
            list.append(entry);
          }

          const deleteButton = document.createElement("button");
          deleteButton.className = "danger";
          deleteButton.type = "button";
          deleteButton.textContent = state.streamerDirectoryDeletingName === streamer.name ? "Deleting..." : "Delete Selected";
          deleteButton.disabled = (
            state.streamerDirectoryDeletingName === streamer.name
            || getStreamerDirectorySelections(streamer.name).length === 0
          );
          deleteButton.addEventListener("click", async () => {
            await deleteStreamerRecordingDirectories(streamer.name);
          });

          shell.append(list, deleteButton);
        }
      }

      directories.append(shell);
      main.append(directories);
    }

    item.append(main);
    elements.streamersList.append(item);
  }
}

function filterAndSortRecordings(recordings) {
  let result = [...recordings];

  switch (state.recordingFilter) {
    case "recent_24h":
      result = result.filter((recording) => isRecent(recording.modified_at, 24));
      break;
    case "processing":
      result = result.filter((recording) => getRecordingStatus(recording).tone === "processing" || recording.is_recording);
      break;
    case "failed":
      result = result.filter((recording) => getRecordingStatus(recording).tone === "failed");
      break;
    case "ready":
      result = result.filter((recording) => getRecordingStatus(recording).tone === "ready");
      break;
    case "all":
    default:
      break;
  }

  result.sort((left, right) => {
    if (state.recordingSort === "channel_asc") {
      return String(left.channel || "").localeCompare(String(right.channel || ""));
    }
    if (state.recordingSort === "ad_break_desc") {
      return Number(right.ad_break_count || 0) - Number(left.ad_break_count || 0);
    }
    if (state.recordingSort === "size_desc") {
      return Number(right.size_bytes || 0) - Number(left.size_bytes || 0);
    }
    return parseDate(right.modified_at) - parseDate(left.modified_at);
  });

  return result;
}

function createRecordingTableRow(recording) {
  const status = getRecordingStatus(recording);
  const row = document.createElement("tr");
  row.className = "recording-row";
  if (state.highlightedRecordingIds.has(recording.recording_id)) {
    row.classList.add("is-updated");
  }

  const channelCell = document.createElement("td");
  const channelStack = document.createElement("div");
  channelStack.className = "channel-stack";

  const channelName = document.createElement("div");
  channelName.className = "channel-name";
  channelName.textContent = recording.channel || "N/A";

  const recordingId = document.createElement("div");
  recordingId.className = "subtle-line monospace";
  recordingId.textContent = recording.recording_id;

  const artifact = document.createElement("div");
  artifact.className = "subtle-line";
  artifact.textContent = `${formatState(recording.artifact_mode)} • ${toFileName(recording.file_path)}`;

  channelStack.append(channelName, recordingId, artifact);
  channelCell.append(channelStack);

  const timeCell = document.createElement("td");
  const timeWrap = document.createElement("div");
  timeWrap.className = "recording-time";
  const modified = document.createElement("div");
  modified.textContent = formatDate(recording.modified_at);
  const modifiedMeta = document.createElement("div");
  modifiedMeta.className = "recording-muted";
  modifiedMeta.textContent = `Updated ${formatRelativeTime(recording.modified_at)}`;
  timeWrap.append(modified, modifiedMeta);
  timeCell.append(timeWrap);

  const adCountCell = document.createElement("td");
  adCountCell.textContent = String(recording.ad_break_count ?? 0);

  const statusCell = document.createElement("td");
  statusCell.append(createStatusBadge(status.tone, status.label));

  const statusMeta = document.createElement("div");
  statusMeta.className = "recording-muted";
  statusMeta.textContent = status.summary;
  statusCell.append(statusMeta);

  if (recording.clean_export_state && recording.clean_export_state !== "none") {
    const exportMeta = document.createElement("div");
    exportMeta.className = "recording-muted";
    exportMeta.textContent = `Export: ${formatState(recording.clean_export_state)}`;
    statusCell.append(exportMeta);
  }

  const errorDetails = createRecordingErrorDetails(recording);
  if (errorDetails) {
    statusCell.append(errorDetails);
  }

  const directoryCell = document.createElement("td");
  const directoryWrap = document.createElement("div");
  directoryWrap.className = "recording-path-wrap";

  const directoryText = document.createElement("div");
  directoryText.className = "path-text";
  directoryText.textContent = recording.clean_export_dir_path || "Pending";

  const pathMeta = document.createElement("div");
  pathMeta.className = "recording-muted";
  pathMeta.textContent = recording.clean_export_dir_path ? "MP4 export directory" : "Directory not ready yet";

  directoryWrap.append(directoryText, pathMeta);
  directoryCell.append(directoryWrap);

  const sizeCell = document.createElement("td");
  const sizeWrap = document.createElement("div");
  sizeWrap.className = "recording-size";
  const sizeText = document.createElement("div");
  sizeText.textContent = formatBytes(recording.size_bytes);
  const sizeMeta = document.createElement("div");
  sizeMeta.className = "recording-muted";
  sizeMeta.textContent = recording.is_recording ? "Growing" : "Final size";
  sizeWrap.append(sizeText, sizeMeta);
  sizeCell.append(sizeWrap);

  const actionsCell = document.createElement("td");
  actionsCell.append(createRecordingActions(recording));

  row.append(channelCell, timeCell, adCountCell, statusCell, directoryCell, sizeCell, actionsCell);
  return row;
}

function createRecordingCard(recording) {
  const status = getRecordingStatus(recording);
  const card = document.createElement("article");
  card.className = "recording-card";
  if (state.highlightedRecordingIds.has(recording.recording_id)) {
    card.classList.add("is-updated");
  }

  const head = document.createElement("div");
  head.className = "recording-card-head";

  const titleWrap = document.createElement("div");
  const channelName = document.createElement("div");
  channelName.className = "channel-name";
  channelName.textContent = recording.channel || "N/A";
  const recordingId = document.createElement("div");
  recordingId.className = "subtle-line monospace";
  recordingId.textContent = recording.recording_id;
  titleWrap.append(channelName, recordingId);

  head.append(titleWrap, createStatusBadge(status.tone, status.label));

  const grid = document.createElement("div");
  grid.className = "recording-card-grid";

  const fields = [
    ["Time", formatDate(recording.modified_at)],
    ["Ad Count", String(recording.ad_break_count ?? 0)],
    ["MP4 Directory", recording.clean_export_dir_path || "Pending"],
    ["Size", formatBytes(recording.size_bytes)],
    ["Artifact", formatState(recording.artifact_mode)],
    ["Output", status.summary],
  ];

  for (const [label, value] of fields) {
    const block = document.createElement("div");
    block.className = "recording-card-stat";
    const labelNode = document.createElement("div");
    labelNode.className = "recording-card-label";
    labelNode.textContent = label;
    const valueNode = document.createElement("div");
    valueNode.className = label === "MP4 Directory" ? "path-text" : "";
    valueNode.textContent = value;
    block.append(labelNode, valueNode);
    grid.append(block);
  }

  card.append(head, grid, createRecordingActions(recording));

  const errorDetails = createRecordingErrorDetails(recording);
  if (errorDetails) {
    card.append(errorDetails);
  }

  return card;
}

function renderRecordings() {
  const recordings = filterAndSortRecordings(state.recordings);
  elements.recordingsBody.replaceChildren();
  elements.recordingsCards.replaceChildren();
  elements.recordingsEmpty.hidden = recordings.length > 0;
  elements.recordingsTable.hidden = recordings.length === 0;
  elements.recordingsTable.parentElement.hidden = recordings.length === 0;

  for (const button of elements.recordingFilters.querySelectorAll("[data-filter]")) {
    button.classList.toggle("is-active", button.dataset.filter === state.recordingFilter);
  }
  elements.recordingSort.value = state.recordingSort;

  if (!recordings.length) {
    return;
  }

  for (const recording of recordings) {
    elements.recordingsBody.append(createRecordingTableRow(recording));
    elements.recordingsCards.append(createRecordingCard(recording));
  }
}

async function refreshStreamers() {
  state.streamers = await request("/streamers");
  renderStreamers();
  renderSummaryCards();
}

async function refreshStatuses() {
  state.statuses = await request("/status");
  renderStatuses();
  renderStreamers();
  renderSummaryCards();
}

async function refreshRecordings() {
  const nextRecordings = await request("/recordings");
  markUpdatedRecordings(state.recordings, nextRecordings);
  state.recordings = nextRecordings;
  renderRecordings();
  renderStatuses();
  renderSummaryCards();

  const needsFollowup = state.recordings.some((recording) => {
    if (recording.is_recording) {
      return true;
    }
    const exportState = String(recording.clean_export_state || "none").toLowerCase();
    const compactState = String(recording.clean_compact_state || "none").toLowerCase();
    return (
      exportState === "queued"
      || exportState === "processing"
      || compactState === "queued"
      || compactState === "processing"
    );
  });

  if (needsFollowup) {
    scheduleRecordingsFollowupRefresh();
  } else {
    clearRecordingsFollowupRefresh();
  }
}

async function refreshAllData({ silent = false } = {}) {
  state.refreshInFlight = true;
  try {
    await request("/refresh", { method: "POST" });
    await Promise.all([refreshStreamers(), refreshStatuses(), refreshRecordings()]);
  } finally {
    state.refreshInFlight = false;
  }
  if (!silent) {
    showToast("Dashboard refreshed");
  }
}

elements.streamerForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const name = elements.streamerName.value.trim();
  if (!name) {
    return;
  }

  try {
    await request("/streamers", {
      method: "POST",
      body: JSON.stringify({ name }),
    });
    elements.streamerName.value = "";
    showToast(`Added ${name}`);
    await refreshAllData();
  } catch (error) {
    showToast(error.message);
  }
});

elements.statusFilters.addEventListener("click", (event) => {
  const button = event.target.closest("[data-filter]");
  if (!button) {
    return;
  }
  state.statusFilter = button.dataset.filter;
  renderStatuses();
});

elements.recordingFilters.addEventListener("click", (event) => {
  const button = event.target.closest("[data-filter]");
  if (!button) {
    return;
  }
  state.recordingFilter = button.dataset.filter;
  renderRecordings();
});

elements.recordingSort.addEventListener("change", () => {
  state.recordingSort = elements.recordingSort.value;
  renderRecordings();
});

elements.recordingsTable.hidden = true;
elements.recordingsTable.parentElement.hidden = true;
startPollingLoop();

refreshAllData().catch((error) => {
  showToast(error.message);
});
