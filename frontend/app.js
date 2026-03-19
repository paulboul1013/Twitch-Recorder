const pollingEnabledKey = "twitch-recorder-auto-refresh-enabled";
const pollIntervalMs = 15000;
const apiBaseUrl = "/api";

const state = {
  pollingEnabled: localStorage.getItem(pollingEnabledKey) !== "false",
  pollingCountdownSeconds: Math.floor(pollIntervalMs / 1000),
  pollingTimerId: null,
  pollingCountdownId: null,
  recordingsFollowupTimerId: null,
  refreshInFlight: false,
  statusCarouselIndex: 0,
};

const elements = {
  togglePolling: document.querySelector("#togglePolling"),
  pollingStatus: document.querySelector("#pollingStatus"),
  refreshAll: document.querySelector("#refreshAll"),
  streamerForm: document.querySelector("#streamerForm"),
  streamerName: document.querySelector("#streamerName"),
  streamersList: document.querySelector("#streamersList"),
  streamersEmpty: document.querySelector("#streamersEmpty"),
  streamerCount: document.querySelector("#streamerCount"),
  summaryRecording: document.querySelector("#summaryRecording"),
  summaryLive: document.querySelector("#summaryLive"),
  summaryMonitored: document.querySelector("#summaryMonitored"),
  refreshStatus: document.querySelector("#refreshStatus"),
  statusCards: document.querySelector("#statusCards"),
  statusEmpty: document.querySelector("#statusEmpty"),
  refreshRecordings: document.querySelector("#refreshRecordings"),
  recordingsTable: document.querySelector("#recordingsTable"),
  recordingsBody: document.querySelector("#recordingsTable tbody"),
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
      const data = await response.json();
      detail = data.detail || detail;
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
  showToast.timeoutId = setTimeout(() => {
    elements.toast.hidden = true;
  }, 2600);
}

function setPollingLabel() {
  elements.togglePolling.textContent = state.pollingEnabled ? "Pause Auto Refresh" : "Resume Auto Refresh";
  if (!state.pollingEnabled) {
    elements.pollingStatus.textContent = "Auto refresh paused";
    return;
  }
  if (document.hidden) {
    elements.pollingStatus.textContent = "Auto refresh paused while tab is hidden";
    return;
  }
  if (state.refreshInFlight) {
    elements.pollingStatus.textContent = "Refreshing now...";
    return;
  }
  elements.pollingStatus.textContent = `Auto refresh in ${state.pollingCountdownSeconds}s`;
}

function stopPollingLoop() {
  clearInterval(state.pollingTimerId);
  clearInterval(state.pollingCountdownId);
  state.pollingTimerId = null;
  state.pollingCountdownId = null;
}

async function runAutoRefresh() {
  if (!state.pollingEnabled || document.hidden || state.refreshInFlight) {
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
  state.pollingCountdownSeconds = Math.floor(pollIntervalMs / 1000);
  setPollingLabel();
  if (!state.pollingEnabled) {
    return;
  }

  state.pollingCountdownId = window.setInterval(() => {
    if (!state.pollingEnabled) {
      return;
    }
    if (document.hidden || state.refreshInFlight) {
      setPollingLabel();
      return;
    }
    state.pollingCountdownSeconds = Math.max(0, state.pollingCountdownSeconds - 1);
    setPollingLabel();
  }, 1000);

  state.pollingTimerId = window.setInterval(async () => {
    if (!state.pollingEnabled || document.hidden) {
      setPollingLabel();
      return;
    }
    state.pollingCountdownSeconds = Math.floor(pollIntervalMs / 1000);
    setPollingLabel();
    await runAutoRefresh();
  }, pollIntervalMs);
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
  return value.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
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

function triggerDownload(path) {
  window.open(`${apiBaseUrl}${path}`, "_blank", "noopener");
}

function toFileName(pathValue) {
  if (!pathValue) {
    return "N/A";
  }
  const parts = String(pathValue).split("/");
  return parts[parts.length - 1] || "N/A";
}

function sleep(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
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

async function waitForCleanExportReady(recordingId, { timeoutMs = 120000, intervalMs = 1500 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const status = await request(`/recordings/${encodeURIComponent(recordingId)}/exports/clean-mp4`);
    const exportState = String(status.state || "").toLowerCase();
    if (exportState === "ready") {
      return;
    }
    if (exportState === "failed") {
      throw new Error(status.error || "Clean MP4 export failed");
    }
    await sleep(intervalMs);
  }
  throw new Error("Clean MP4 export is still processing, please try again shortly.");
}

function getCleanExportStatus(recording) {
  if (recording.is_recording) {
    return {
      text: "Recording",
      tone: "watchable-pending",
    };
  }
  if (recording.artifact_mode !== "segment_native") {
    return {
      text: "Legacy recording",
      tone: "watchable-ready",
    };
  }
  if (Number(recording.clean_segment_count || 0) <= 0) {
    return {
      text: "No clean content",
      tone: "watchable-failed",
    };
  }
  const exportState = String(recording.clean_export_state || "none").toLowerCase();
  const compactState = String(recording.clean_compact_state || "none").toLowerCase();
  if (exportState === "failed" || compactState === "failed") {
    return {
      text: "Export failed",
      tone: "watchable-failed",
    };
  }
  if (
    exportState === "queued" ||
    exportState === "processing" ||
    compactState === "queued" ||
    compactState === "processing"
  ) {
    return {
      text: "Processing",
      tone: "watchable-pending",
    };
  }
  if (compactState === "ready" || exportState === "ready") {
    return {
      text: "Ready to export MP4",
      tone: "watchable-ready",
    };
  }
  return {
    text: "Processing",
    tone: "watchable-pending",
  };
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
  fallback.setAttribute("aria-hidden", "true");
  fallback.textContent = (name || "?").slice(0, 1).toUpperCase();
  return fallback;
}

function createStatusDetailRow(labelText, valueText) {
  const row = document.createElement("div");
  const label = document.createElement("strong");
  label.textContent = `${labelText}:`;
  const valueNode = document.createTextNode(` ${valueText}`);
  row.append(label, valueNode);
  return row;
}

function shiftStatusCarousel(statuses, step) {
  if (!statuses.length) {
    state.statusCarouselIndex = 0;
    return;
  }
  const nextIndex = state.statusCarouselIndex + step;
  state.statusCarouselIndex = (nextIndex + statuses.length) % statuses.length;
  renderStatuses(statuses);
}

function renderStreamers(streamers) {
  elements.streamerCount.textContent = String(streamers.length);
  elements.streamersList.replaceChildren();
  elements.streamersEmpty.hidden = streamers.length > 0;

  for (const streamer of streamers) {
    const item = document.createElement("li");
    item.className = "list-item";

    const label = document.createElement("span");
    label.className = "channel";
    label.textContent = streamer.name;

    const removeButton = document.createElement("button");
    removeButton.className = "secondary";
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

    item.append(label, removeButton);
    elements.streamersList.append(item);
  }
}

function renderStatuses(statuses) {
  elements.statusCards.replaceChildren();
  elements.statusEmpty.hidden = statuses.length > 0;
  elements.summaryRecording.textContent = String(statuses.filter((status) => status.is_recording).length);
  elements.summaryLive.textContent = String(statuses.filter((status) => status.is_live).length);

  if (!statuses.length) {
    state.statusCarouselIndex = 0;
    return;
  }

  state.statusCarouselIndex = Math.max(0, Math.min(state.statusCarouselIndex, statuses.length - 1));
  const status = statuses[state.statusCarouselIndex];

  const card = document.createElement("article");
  card.className = "status-card";

  const top = document.createElement("div");
  top.className = "status-top";

  const name = document.createElement("strong");
  name.className = "channel";
  name.textContent = status.name;

  const badges = document.createElement("div");
  badges.className = "status-badges";
  const liveBadge = document.createElement("span");
  liveBadge.className = `badge ${status.is_live ? "live" : "offline"}`;
  liveBadge.textContent = status.is_live ? "LIVE" : "OFFLINE";

  badges.append(liveBadge);
  if (status.is_recording) {
    const recordingBadge = document.createElement("span");
    recordingBadge.className = "badge recording";
    recordingBadge.textContent = "RECORDING";
    badges.append(recordingBadge);
  }

  const heading = document.createElement("div");
  heading.className = "status-heading";

  const identity = document.createElement("div");
  identity.className = "status-identity";
  identity.append(createAvatar(status.name, status.profile_image_url), name);

  if (status.is_recording) {
    const liveMotion = document.createElement("div");
    liveMotion.className = "recording-motion";
    liveMotion.setAttribute("aria-hidden", "true");
    const recordingDot = document.createElement("span");
    recordingDot.className = "recording-dot";
    liveMotion.append(recordingDot);
    for (let index = 0; index < 3; index += 1) {
      const recordingWave = document.createElement("span");
      recordingWave.className = "recording-wave";
      liveMotion.append(recordingWave);
    }
    identity.append(liveMotion);
  }

  heading.append(identity, badges);

  const actions = document.createElement("div");
  actions.className = "status-actions";
  if (status.is_recording) {
    const stopButton = document.createElement("button");
    stopButton.className = "danger";
    stopButton.textContent = "Stop Recording";
    stopButton.addEventListener("click", async () => {
      stopButton.disabled = true;
      stopButton.textContent = "Stopping...";
      try {
        const result = await request(`/streamers/${encodeURIComponent(status.name)}/stop`, {
          method: "POST",
        });
        if (result.stopped) {
          showToast(`Stopped recording for ${status.name}`);
        } else {
          showToast(`No active recording for ${status.name}`);
        }
        await refreshAllData();
      } catch (error) {
        stopButton.disabled = false;
        stopButton.textContent = "Stop Recording";
        showToast(error.message);
      }
    });
    actions.append(stopButton);
  } else if (status.is_live) {
    const startButton = document.createElement("button");
    startButton.textContent = "Start Recording";
    startButton.addEventListener("click", async () => {
      startButton.disabled = true;
      startButton.textContent = "Starting...";
      try {
        const result = await request(`/streamers/${encodeURIComponent(status.name)}/start`, {
          method: "POST",
        });
        if (result.started) {
          showToast(`Started recording for ${status.name}`);
        } else {
          showToast(`Could not start recording for ${status.name}`);
        }
        await refreshAllData();
      } catch (error) {
        startButton.disabled = false;
        startButton.textContent = "Start Recording";
        showToast(error.message);
      }
    });
    actions.append(startButton);
  }

  top.append(heading, actions);

  const details = document.createElement("div");
  details.className = "status-details";
  details.append(
    createStatusDetailRow(
      "Recording State",
      formatState(normalizeRecordingState(status.recording_state)),
    ),
    createStatusDetailRow("Title", status.title || "N/A"),
    createStatusDetailRow("Game", status.game_name || "N/A"),
    createStatusDetailRow("Viewers", String(status.viewer_count ?? "N/A")),
    createStatusDetailRow("Live Started", formatDate(status.started_at)),
    createStatusDetailRow("Checked", formatDate(status.last_checked_at)),
    createStatusDetailRow("Offline Since", formatDate(status.offline_since)),
    createStatusDetailRow("Stop After", formatDate(status.stop_after_at)),
    createStatusDetailRow("Recording Started", formatDate(status.recording_started_at)),
    createStatusDetailRow("Recording Ended", formatDate(status.recording_ended_at)),
    createStatusDetailRow("Exit Code", String(status.recording_exit_code ?? "N/A")),
    createStatusDetailRow("Output", status.output_path || "N/A"),
    createStatusDetailRow("Error", status.last_error || "None"),
  );

  card.append(top, details);

  if (statuses.length > 1) {
    const carouselControls = document.createElement("div");
    carouselControls.className = "status-carousel";

    const previousButton = document.createElement("button");
    previousButton.className = "secondary status-carousel-button";
    previousButton.type = "button";
    previousButton.textContent = "<";
    previousButton.setAttribute("aria-label", "Show previous streamer");
    previousButton.addEventListener("click", () => {
      shiftStatusCarousel(statuses, -1);
    });

    const statusPosition = document.createElement("div");
    statusPosition.className = "status-carousel-position";
    statusPosition.textContent = `${state.statusCarouselIndex + 1} / ${statuses.length}`;

    const nextButton = document.createElement("button");
    nextButton.className = "secondary status-carousel-button";
    nextButton.type = "button";
    nextButton.textContent = ">";
    nextButton.setAttribute("aria-label", "Show next streamer");
    nextButton.addEventListener("click", () => {
      shiftStatusCarousel(statuses, 1);
    });

    carouselControls.append(previousButton, statusPosition, nextButton);
    card.append(carouselControls);
  }

  elements.statusCards.append(card);
}

function renderRecordings(recordings) {
  const visibleRecordings = recordings.slice(0, 5);
  elements.recordingsBody.replaceChildren();
  elements.recordingsEmpty.hidden = visibleRecordings.length > 0;
  elements.recordingsTable.hidden = visibleRecordings.length === 0;

  for (const recording of visibleRecordings) {
    const exportStatus = getCleanExportStatus(recording);
    const isSegmentNative = recording.artifact_mode === "segment_native";
    const isRecording = Boolean(recording.is_recording);
    const cleanExportState = String(recording.clean_export_state || "none").toLowerCase();
    const isPreparing = cleanExportState === "queued" || cleanExportState === "processing";
    const isReady = cleanExportState === "ready";
    const hasCleanContent = Number(recording.clean_segment_count || 0) > 0;
    const row = document.createElement("tr");

    const channelCell = document.createElement("td");
    channelCell.className = "channel";
    const channelName = document.createElement("div");
    channelName.textContent = recording.channel || "N/A";
    channelCell.append(channelName);

    const fileMeta = document.createElement("div");
    fileMeta.className = "recording-meta";
    const sourceFileName = recording.source_file_name || recording.file_name || "N/A";
    const displayFileName = isRecording
      ? sourceFileName
      : recording.watchable_file_name || sourceFileName;
    fileMeta.textContent = `${displayFileName} (${formatBytes(recording.size_bytes)})`;
    channelCell.append(fileMeta);

    if (isRecording) {
      const activeHint = document.createElement("div");
      activeHint.className = "recording-meta";
      activeHint.textContent = "Recording in progress";
      channelCell.append(activeHint);
    }

    const exportCell = document.createElement("td");
    const exportLabel = document.createElement("span");
    exportLabel.className = `watchable-status ${exportStatus.tone}`;
    exportLabel.textContent = exportStatus.text;
    exportCell.append(exportLabel);

    if (recording.unknown_ad_confidence && isSegmentNative) {
      const confidenceHint = document.createElement("div");
      confidenceHint.className = "recording-meta";
      confidenceHint.textContent = "No ads detected";
      exportCell.append(confidenceHint);
    }

    if (recording.clean_export_error) {
      const exportError = document.createElement("div");
      exportError.className = "recording-meta";
      exportError.textContent = recording.clean_export_error;
      exportCell.append(exportError);
    }

    const downloadCleanButton = document.createElement("button");
    downloadCleanButton.className = "secondary";
    if (isPreparing) {
      downloadCleanButton.textContent = "Preparing...";
    } else if (isRecording) {
      downloadCleanButton.textContent = "Recording...";
    } else if (!hasCleanContent) {
      downloadCleanButton.textContent = "No clean content";
    } else {
      downloadCleanButton.textContent = "Download Clean MP4";
    }
    downloadCleanButton.disabled = !isSegmentNative || isPreparing || isRecording || !hasCleanContent;
    downloadCleanButton.addEventListener("click", async () => {
      if (!recording.recording_id) {
        showToast("Recording id is missing");
        return;
      }
      if (!hasCleanContent) {
        showToast("No clean content is available for this recording");
        return;
      }
      if (isReady) {
        triggerDownload(`/recordings/${encodeURIComponent(recording.recording_id)}/download/clean-mp4`);
        return;
      }
      downloadCleanButton.disabled = true;
      downloadCleanButton.textContent = "Preparing...";
      try {
        await request(`/recordings/${encodeURIComponent(recording.recording_id)}/exports/clean-mp4`, {
          method: "POST",
        });
        showToast(`Preparing Clean MP4 for ${recording.channel}`);
        await refreshRecordings();
        await waitForCleanExportReady(recording.recording_id);
        await refreshRecordings();
        triggerDownload(`/recordings/${encodeURIComponent(recording.recording_id)}/download/clean-mp4`);
      } catch (error) {
        downloadCleanButton.disabled = false;
        downloadCleanButton.textContent = "Download Clean MP4";
        showToast(error.message);
      }
    });
    exportCell.append(downloadCleanButton);

    const modifiedCell = document.createElement("td");
    modifiedCell.textContent = formatDate(recording.modified_at);

    row.append(channelCell, exportCell, modifiedCell);
    elements.recordingsBody.append(row);
  }
}

async function refreshStreamers() {
  const streamers = await request("/streamers");
  elements.summaryMonitored.textContent = String(streamers.length);
  renderStreamers(streamers);
}

async function refreshStatuses() {
  const statuses = await request("/status");
  renderStatuses(statuses);
}

async function refreshRecordings() {
  const recordings = await request("/recordings");
  renderRecordings(recordings);
  const needsFollowup = recordings.some((recording) => {
    if (recording.is_recording) {
      return true;
    }
    const exportState = String(recording.clean_export_state || "none").toLowerCase();
    const compactState = String(recording.clean_compact_state || "none").toLowerCase();
    return (
      exportState === "queued" ||
      exportState === "processing" ||
      compactState === "queued" ||
      compactState === "processing"
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
  setPollingLabel();
  try {
    await request("/refresh", { method: "POST" });
    await Promise.all([refreshStreamers(), refreshStatuses(), refreshRecordings()]);
    state.pollingCountdownSeconds = Math.floor(pollIntervalMs / 1000);
  } finally {
    state.refreshInFlight = false;
    setPollingLabel();
  }
  if (!silent) {
    showToast("Dashboard refreshed");
  }
}

elements.refreshAll.addEventListener("click", async () => {
  try {
    await refreshAllData();
  } catch (error) {
    showToast(error.message);
  }
});

elements.refreshStatus.addEventListener("click", async () => {
  try {
    await request("/refresh", { method: "POST" });
    await Promise.all([refreshStatuses(), refreshRecordings()]);
    showToast("Status refreshed");
  } catch (error) {
    showToast(error.message);
  }
});

elements.refreshRecordings.addEventListener("click", async () => {
  try {
    await refreshRecordings();
    showToast("Recordings refreshed");
  } catch (error) {
    showToast(error.message);
  }
});

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

elements.togglePolling.addEventListener("click", () => {
  state.pollingEnabled = !state.pollingEnabled;
  localStorage.setItem(pollingEnabledKey, String(state.pollingEnabled));
  state.pollingCountdownSeconds = Math.floor(pollIntervalMs / 1000);
  setPollingLabel();
  startPollingLoop();
});

document.addEventListener("visibilitychange", () => {
  state.pollingCountdownSeconds = Math.floor(pollIntervalMs / 1000);
  setPollingLabel();
});

elements.recordingsTable.hidden = true;
setPollingLabel();
startPollingLoop();

refreshAllData().catch((error) => {
  showToast(error.message);
});
