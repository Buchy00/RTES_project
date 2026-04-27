#!/usr/bin/env python3
"""Web dashboard sourced strictly from ROS 2 topics.

This process does two things:
1) Subscribes to ROS topics and stores a rolling history in memory.
2) Serves a local web dashboard and JSON API.

Expected ROS topics:
- /distance (std_msgs/Float32)
- /distance_filtered (std_msgs/Float32)
- /fault (std_msgs/Bool)
- /parking_state (std_msgs/String)
- /sequence (std_msgs/Int32)
"""

import argparse
import json
import os
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Deque, Dict, List
from urllib.parse import parse_qs, urlparse

try:
  import rclpy
  from rclpy.executors import SingleThreadedExecutor
  from rclpy.node import Node
  from std_msgs.msg import Bool, Float32, Int32, String
except ImportError as exc:
  print("[ros-web] failed to import ROS 2 Python dependencies:", file=sys.stderr)
  print(f"[ros-web] {exc}", file=sys.stderr)
  print("[ros-web] fix: source your ROS distro before running this script.", file=sys.stderr)
  print("[ros-web] example:", file=sys.stderr)
  print("[ros-web]   source /opt/ros/humble/setup.bash", file=sys.stderr)
  print("[ros-web]   source ~/Desktop/RTES_project/rtes_env/bin/activate", file=sys.stderr)
  print("[ros-web]   export RMW_IMPLEMENTATION=rmw_fastrtps_cpp", file=sys.stderr)
  print("[ros-web]   python3 host_tools/ros_web_dashboard.py", file=sys.stderr)
  print(
    "[ros-web] if librmw_fastrtps_cpp.so is still missing, install: "
    "sudo apt install ros-humble-rmw-fastrtps-cpp",
    file=sys.stderr,
  )
  print(f"[ros-web] LD_LIBRARY_PATH={os.environ.get('LD_LIBRARY_PATH', '')}", file=sys.stderr)
  raise SystemExit(1)


@dataclass
class Point:
    t: float
    value: float


class SharedTelemetry:
    def __init__(self, max_points: int) -> None:
        self.max_points = max_points
        self.lock = threading.Lock()
        self.raw: Deque[Point] = deque(maxlen=max_points)
        self.filtered: Deque[Point] = deque(maxlen=max_points)
        self.fault: Deque[Point] = deque(maxlen=max_points)
        self.state_code: Deque[Point] = deque(maxlen=max_points)
        self.latest_state = "UNKNOWN"
        self.latest_sequence = -1
        self.last_update = 0.0

    def push_raw(self, value: float) -> None:
        now = time.time()
        with self.lock:
            self.raw.append(Point(now, value))
            self.last_update = now

    def push_filtered(self, value: float) -> None:
        now = time.time()
        with self.lock:
            self.filtered.append(Point(now, value))
            self.last_update = now

    def push_fault(self, value: bool) -> None:
        now = time.time()
        with self.lock:
            self.fault.append(Point(now, 1.0 if value else 0.0))
            self.last_update = now

    def push_state(self, state_text: str) -> None:
        state_map = {
            "SAFE": 0.0,
            "WARNING": 1.0,
            "DANGER": 2.0,
            "FAULT": 3.0,
        }
        now = time.time()
        with self.lock:
            self.latest_state = state_text
            self.state_code.append(Point(now, state_map.get(state_text, 4.0)))
            self.last_update = now

    def set_sequence(self, sequence: int) -> None:
        with self.lock:
            self.latest_sequence = sequence

    def snapshot(self, window_seconds: float) -> Dict[str, object]:
        now = time.time()
        start_t = now - window_seconds

        def clip(points: Deque[Point]) -> List[List[float]]:
            xs: List[float] = []
            ys: List[float] = []
            for p in points:
                if p.t >= start_t:
                    xs.append(p.t - start_t)
                    ys.append(p.value)
            return [xs, ys]

        with self.lock:
            raw = clip(self.raw)
            filtered = clip(self.filtered)
            fault = clip(self.fault)
            state_code = clip(self.state_code)
            latest_state = self.latest_state
            latest_sequence = self.latest_sequence
            last_update = self.last_update

        age_ms = -1
        if last_update > 0.0:
            age_ms = int((now - last_update) * 1000)

        return {
            "window_seconds": window_seconds,
            "raw": raw,
            "filtered": filtered,
            "fault": fault,
            "state_code": state_code,
            "latest_state": latest_state,
            "latest_sequence": latest_sequence,
            "last_update_age_ms": age_ms,
        }


class RosTopicCollector(Node):
    def __init__(self, shared: SharedTelemetry) -> None:
        super().__init__("rtes_ros_web_dashboard")
        self.shared = shared

        qos_depth = 10
        self.create_subscription(Float32, "/distance", self._on_distance, qos_depth)
        self.create_subscription(Float32, "/distance_filtered", self._on_filtered, qos_depth)
        self.create_subscription(Bool, "/fault", self._on_fault, qos_depth)
        self.create_subscription(String, "/parking_state", self._on_state, qos_depth)
        self.create_subscription(Int32, "/sequence", self._on_sequence, qos_depth)

        self.get_logger().info("ROS subscriptions active for web dashboard")

    def _on_distance(self, msg: Float32) -> None:
        self.shared.push_raw(float(msg.data))

    def _on_filtered(self, msg: Float32) -> None:
        self.shared.push_filtered(float(msg.data))

    def _on_fault(self, msg: Bool) -> None:
        self.shared.push_fault(bool(msg.data))

    def _on_state(self, msg: String) -> None:
        self.shared.push_state(msg.data)

    def _on_sequence(self, msg: Int32) -> None:
        self.shared.set_sequence(int(msg.data))


def make_handler(shared: SharedTelemetry):
    html = """<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>RTES ROS Dashboard</title>
  <style>
    :root {
      --bg: #f3f5f7;
      --panel: #ffffff;
      --ink: #1a2430;
      --muted: #5f6b77;
      --line1: #0c7bdc;
      --line2: #f46d43;
      --ok: #2e9f55;
      --warn: #e17c05;
      --danger: #c72e36;
      --fault: #6c2eb9;
      --grid: #dde3ea;
    }
    body { margin: 0; background: var(--bg); color: var(--ink); font-family: "Noto Sans", "DejaVu Sans", sans-serif; }
    .wrap { max-width: 1100px; margin: 24px auto; padding: 0 16px; }
    h1 { margin: 0 0 12px; font-size: 1.6rem; }
    .status { background: var(--panel); border-radius: 12px; padding: 12px 14px; margin-bottom: 14px; box-shadow: 0 4px 16px rgba(0,0,0,0.06); }
    .state-banner { background: var(--panel); border-radius: 12px; padding: 14px; margin-bottom: 14px; box-shadow: 0 4px 16px rgba(0,0,0,0.06); border-left: 10px solid var(--muted); }
    .state-title { font-size: 0.82rem; letter-spacing: 0.08em; color: var(--muted); text-transform: uppercase; }
    .state-value { font-size: 2rem; font-weight: 700; margin-top: 4px; }
    .state-action { margin-top: 6px; font-size: 1rem; color: var(--ink); }
    .state-safe { border-left-color: var(--ok); }
    .state-warning { border-left-color: var(--warn); }
    .state-danger { border-left-color: var(--danger); }
    .state-fault { border-left-color: var(--fault); }
    .state-unknown { border-left-color: var(--muted); }
    .grid { display: grid; grid-template-columns: repeat(2, minmax(280px, 1fr)); gap: 14px; }
    .card { background: var(--panel); border-radius: 12px; padding: 10px; box-shadow: 0 4px 16px rgba(0,0,0,0.06); }
    canvas { width: 100%; height: 220px; display: block; }
    .legend { font-size: 0.9rem; color: var(--muted); margin-top: 4px; }
    @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class=\"wrap\">
    <h1>RTES ROS Topic Dashboard</h1>
    <div class=\"status\" id=\"status\">Waiting for ROS topic data...</div>
    <div class=\"state-banner state-unknown\" id=\"stateBanner\">
      <div class=\"state-title\">Parking Decision</div>
      <div class=\"state-value\" id=\"stateValue\">UNKNOWN</div>
      <div class=\"state-action\" id=\"stateAction\">Awaiting ROS state messages...</div>
    </div>
    <div class=\"grid\">
      <div class=\"card\">
        <canvas id=\"distanceCanvas\" width=\"1000\" height=\"260\"></canvas>
        <div class=\"legend\">Blue: raw distance, Orange: filtered distance</div>
      </div>
      <div class=\"card\">
        <canvas id=\"faultCanvas\" width=\"1000\" height=\"260\"></canvas>
        <div class=\"legend\">Fault signal (0/1)</div>
      </div>
      <div class=\"card\">
        <canvas id=\"stateCanvas\" width=\"1000\" height=\"260\"></canvas>
        <div class=\"legend\">State code: SAFE=0, WARNING=1, DANGER=2, FAULT=3</div>
      </div>
    </div>
  </div>

<script>
const colors = {
  raw: "#0c7bdc",
  filtered: "#f46d43",
  fault: "#c72e36",
  state: "#6c2eb9",
  grid: "#dde3ea",
  axis: "#5f6b77",
};

function clearCanvas(ctx) {
  ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
}

function drawAxes(ctx, w, h, ymin, ymax, yLabel) {
  const left = 50, right = w - 15, top = 15, bottom = h - 30;
  ctx.strokeStyle = colors.grid;
  ctx.lineWidth = 1;

  for (let i = 0; i <= 4; i++) {
    const y = top + (i * (bottom - top) / 4);
    ctx.beginPath();
    ctx.moveTo(left, y);
    ctx.lineTo(right, y);
    ctx.stroke();
  }

  ctx.strokeStyle = colors.axis;
  ctx.beginPath();
  ctx.moveTo(left, top);
  ctx.lineTo(left, bottom);
  ctx.lineTo(right, bottom);
  ctx.stroke();

  ctx.fillStyle = colors.axis;
  ctx.font = "12px sans-serif";
  ctx.fillText(yLabel, 6, 14);

  for (let i = 0; i <= 4; i++) {
    const y = top + (i * (bottom - top) / 4);
    const v = ymax - (i * (ymax - ymin) / 4);
    ctx.fillText(v.toFixed(1), 6, y + 4);
  }

  return {left, right, top, bottom};
}

function plotLine(ctx, xs, ys, bounds, xmin, xmax, ymin, ymax, color, step) {
  if (!xs.length || !ys.length) return;

  const {left, right, top, bottom} = bounds;
  const xscale = (right - left) / Math.max(1e-6, (xmax - xmin));
  const yscale = (bottom - top) / Math.max(1e-6, (ymax - ymin));

  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();

  let prevX = left + (xs[0] - xmin) * xscale;
  let prevY = bottom - (ys[0] - ymin) * yscale;
  ctx.moveTo(prevX, prevY);

  for (let i = 1; i < xs.length; i++) {
    const x = left + (xs[i] - xmin) * xscale;
    const y = bottom - (ys[i] - ymin) * yscale;
    if (step) {
      ctx.lineTo(x, prevY);
      ctx.lineTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
    prevX = x;
    prevY = y;
  }
  ctx.stroke();
}

function updateStatus(data) {
  const el = document.getElementById("status");
  const age = data.last_update_age_ms;
  const ageText = age >= 0 ? `${age} ms` : "n/a";
  el.textContent = `sequence=${data.latest_sequence} | state=${data.latest_state} | last_msg_age=${ageText}`;
}

function updateStateIndicator(data) {
  const state = data.latest_state || "UNKNOWN";
  const banner = document.getElementById("stateBanner");
  const value = document.getElementById("stateValue");
  const action = document.getElementById("stateAction");

  banner.classList.remove("state-safe", "state-warning", "state-danger", "state-fault", "state-unknown");
  value.textContent = state;

  if (state === "SAFE") {
    banner.classList.add("state-safe");
    action.textContent = "Action: Continue moving. Path is clear.";
  } else if (state === "WARNING") {
    banner.classList.add("state-warning");
    action.textContent = "Action: Slow down and prepare to stop.";
  } else if (state === "DANGER") {
    banner.classList.add("state-danger");
    action.textContent = "Action: Stop immediately to avoid collision.";
  } else if (state === "FAULT") {
    banner.classList.add("state-fault");
    action.textContent = "Action: Sensor fault. Check sensor/system integrity.";
  } else {
    banner.classList.add("state-unknown");
    action.textContent = "Action: Awaiting valid state from ROS topics.";
  }
}

function renderDistance(data) {
  const c = document.getElementById("distanceCanvas");
  const ctx = c.getContext("2d");
  clearCanvas(ctx);

  const rawX = data.raw[0], rawY = data.raw[1];
  const filX = data.filtered[0], filY = data.filtered[1];
  const allY = rawY.concat(filY);

  let ymin = 0, ymax = 220;
  if (allY.length) {
    ymin = Math.min(...allY) - 5;
    ymax = Math.max(...allY) + 5;
    if ((ymax - ymin) < 20) { ymin -= 10; ymax += 10; }
  }

  const xmin = 0;
  const xmax = data.window_seconds;
  const b = drawAxes(ctx, c.width, c.height, ymin, ymax, "cm");

  plotLine(ctx, rawX, rawY, b, xmin, xmax, ymin, ymax, colors.raw, false);
  plotLine(ctx, filX, filY, b, xmin, xmax, ymin, ymax, colors.filtered, false);
}

function renderFault(data) {
  const c = document.getElementById("faultCanvas");
  const ctx = c.getContext("2d");
  clearCanvas(ctx);

  const xs = data.fault[0], ys = data.fault[1];
  const xmin = 0, xmax = data.window_seconds;
  const ymin = -0.1, ymax = 1.1;
  const b = drawAxes(ctx, c.width, c.height, ymin, ymax, "fault");

  plotLine(ctx, xs, ys, b, xmin, xmax, ymin, ymax, colors.fault, true);
}

function renderState(data) {
  const c = document.getElementById("stateCanvas");
  const ctx = c.getContext("2d");
  clearCanvas(ctx);

  const xs = data.state_code[0], ys = data.state_code[1];
  const xmin = 0, xmax = data.window_seconds;
  const ymin = -0.2, ymax = 4.2;
  const b = drawAxes(ctx, c.width, c.height, ymin, ymax, "state");

  plotLine(ctx, xs, ys, b, xmin, xmax, ymin, ymax, colors.state, true);
}

async function poll() {
  try {
    const resp = await fetch("/api/telemetry?window=30");
    const data = await resp.json();
    updateStatus(data);
    updateStateIndicator(data);
    renderDistance(data);
    renderFault(data);
    renderState(data);
  } catch (err) {
    const el = document.getElementById("status");
    el.textContent = `dashboard fetch error: ${err}`;
  }
}

setInterval(poll, 250);
poll();
</script>
</body>
</html>
"""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/":
                body = html.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if parsed.path == "/api/telemetry":
                qs = parse_qs(parsed.query)
                window = 30.0
                if "window" in qs and qs["window"]:
                    try:
                        window = max(5.0, min(120.0, float(qs["window"][0])))
                    except ValueError:
                        window = 30.0

                payload = json.dumps(shared.snapshot(window)).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            self.send_error(HTTPStatus.NOT_FOUND, "not found")

        def log_message(self, fmt, *args):
            return

    return Handler


def start_ros_collector(shared: SharedTelemetry):
    rclpy.init(args=None)
    node = RosTopicCollector(shared)
    executor = SingleThreadedExecutor()
    executor.add_node(node)

    stop_event = threading.Event()

    def run() -> None:
        while not stop_event.is_set():
            executor.spin_once(timeout_sec=0.1)

    t = threading.Thread(target=run, daemon=True)
    t.start()

    return stop_event, t, executor, node


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Web dashboard sourced strictly from ROS 2 topics")
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind host")
    parser.add_argument("--http-port", type=int, default=8080, help="HTTP bind port")
    parser.add_argument("--max-points", type=int, default=1200, help="ring buffer points per series")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    shared = SharedTelemetry(max_points=args.max_points)
    stop_event, spin_thread, executor, node = start_ros_collector(shared)

    try:
        handler = make_handler(shared)
        server = ThreadingHTTPServer((args.host, args.http_port), handler)
        print(f"[web] dashboard serving on http://{args.host}:{args.http_port}")
        print("[web] source is ROS topics only: /distance /distance_filtered /fault /parking_state /sequence")
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            server.server_close()
        except Exception:
            pass
        stop_event.set()
        spin_thread.join(timeout=1.0)
        executor.remove_node(node)
        node.destroy_node()
        rclpy.shutdown()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
