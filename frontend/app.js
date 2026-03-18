const pollingEnabledKey = "twitch-recorder-auto-refresh-enabled";
const pollIntervalMs = 15000;
const apiBaseUrl = "/api";

const state = {
  pollingEnabled: localStorage.getItem(pollingEnabledKey) !== "false",
  pollingCountdownSeconds: Math.floor(pollIntervalMs / 1000),
  pollingTimerId: null,
  pollingCountdownId: null,
  refreshInFlight: false,
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

function formatBytes(size) {
  if (!Number.isFinite(size)) {
    return "N/A";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = size;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(value >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
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

function toFileName(pathValue) {
  if (!pathValue) {
    return "N/A";
  }
  const parts = String(pathValue).split("/");
  return parts[parts.length - 1] || "N/A";
}

function triggerDownload(path) {
  window.open(`${apiBaseUrl}${path}`, "_blank", "noopener");
}

function getCleanExportStatus(recording) {
  if (recording.artifact_mode !== "segment_native") {
    return {
      text: "Legacy recording",
      tone: "watchable-ready",
    };
  }
  const state = String(recording.clean_export_state || "none").toLowerCase();
  if (state === "failed") {
    return {
      text: "Export failed",
      tone: "watchable-failed",
    };
  }
  if (state === "queued" || state === "processing") {
    return {
      text: formatState(state),
      tone: "watchable-pending",
    };
  }
  if (state === "ready") {
    return {
      text: `Ready · ${toFileName(recording.clean_export_path)}`,
      tone: "watchable-ready",
    };
  }
  return {
    text: "Not exported",
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

  for (const status of statuses) {
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
    elements.statusCards.append(card);
  }
}

function renderRecordings(recordings) {
  const visibleRecordings = recordings.slice(0, 5);
  elements.recordingsBody.replaceChildren();
  elements.recordingsEmpty.hidden = visibleRecordings.length > 0;
  elements.recordingsTable.hidden = visibleRecordings.length === 0;

  for (const recording of visibleRecordings) {
    const exportStatus = getCleanExportStatus(recording);
    const row = document.createElement("tr");

    const channelCell = document.createElement("td");
    channelCell.className = "channel";
    channelCell.textContent = recording.channel || "N/A";

    const fullCell = document.createElement("td");
    const fullCode = document.createElement("code");
    fullCode.textContent = toFileName(recording.full_artifact_path || recording.source_file_path);
    fullCell.append(fullCode);
    if (Number.isFinite(recording.size_bytes)) {
      const sizeMeta = document.createElement("div");
      sizeMeta.className = "recording-meta";
      sizeMeta.textContent = formatBytes(recording.size_bytes);
      fullCell.append(sizeMeta);
    }
    const fullDownloadButton = document.createElement("button");
    fullDownloadButton.className = "secondary";
    fullDownloadButton.textContent = "Download Full";
    fullDownloadButton.disabled = !recording.recording_id;
    fullDownloadButton.addEventListener("click", () => {
      triggerDownload(`/recordings/${encodeURIComponent(recording.recording_id)}/download/full`);
    });
    fullCell.append(fullDownloadButton);

    const cleanCell = document.createElement("td");
    const cleanCode = document.createElement("code");
    cleanCode.textContent = toFileName(recording.clean_artifact_path);
    cleanCell.append(cleanCode);
    const cleanDownloadButton = document.createElement("button");
    cleanDownloadButton.className = "secondary";
    cleanDownloadButton.textContent = "Download Clean Manifest";
    cleanDownloadButton.disabled =
      recording.artifact_mode !== "segment_native" || !recording.clean_artifact_path;
    cleanDownloadButton.addEventListener("click", () => {
      triggerDownload(
        `/recordings/${encodeURIComponent(recording.recording_id)}/download/clean-manifest`,
      );
    });
    cleanCell.append(cleanDownloadButton);

    const exportCell = document.createElement("td");
    const exportLabel = document.createElement("span");
    exportLabel.className = `watchable-status ${exportStatus.tone}`;
    exportLabel.textContent = exportStatus.text;
    exportCell.append(exportLabel);

    if (recording.clean_export_error) {
      const exportError = document.createElement("div");
      exportError.className = "recording-meta";
      exportError.textContent = recording.clean_export_error;
      exportCell.append(exportError);
    }

    const exportButton = document.createElement("button");
    exportButton.textContent = "Export Clean MP4";
    exportButton.disabled =
      recording.artifact_mode !== "segment_native" ||
      recording.clean_export_state === "queued" ||
      recording.clean_export_state === "processing";
    exportButton.addEventListener("click", async () => {
      exportButton.disabled = true;
      exportButton.textContent = "Queueing...";
      try {
        await request(`/recordings/${encodeURIComponent(recording.recording_id)}/exports/clean-mp4`, {
          method: "POST",
        });
        showToast(`Export queued for ${recording.channel}`);
        await refreshRecordings();
      } catch (error) {
        exportButton.disabled = false;
        exportButton.textContent = "Export Clean MP4";
        showToast(error.message);
      }
    });
    exportCell.append(exportButton);

    const downloadCleanButton = document.createElement("button");
    downloadCleanButton.className = "secondary";
    downloadCleanButton.textContent = "Download Clean MP4";
    downloadCleanButton.disabled = recording.clean_export_state !== "ready";
    downloadCleanButton.addEventListener("click", () => {
      triggerDownload(`/recordings/${encodeURIComponent(recording.recording_id)}/download/clean-mp4`);
    });
    exportCell.append(downloadCleanButton);

    const modifiedCell = document.createElement("td");
    modifiedCell.textContent = formatDate(recording.modified_at);

    row.append(channelCell, fullCell, cleanCell, exportCell, modifiedCell);
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
    await refreshStatuses();
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
