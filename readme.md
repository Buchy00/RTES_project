# RTES Project - Parking Assist

Real-time embedded parking-assist pipeline on ESP32-S3 (FreeRTOS) with host-side ROS 2 integration.

## What the project does
- Embedded pipeline: sensor -> filter -> validation -> decision -> feedback -> UDP send.
- Host bridge converts raw UDP packets into ROS 2 topics.
- Dashboards read ROS topics only.

## Communication Pipeline
1. ESP32 sends raw UDP telemetry (`main/main.c`, `k_host_ip:k_host_port`).
2. `host_tools/udp_to_ros2_bridge.py` decodes packets.
3. Bridge publishes ROS topics (`/distance`, `/distance_filtered`, `/fault`, `/parking_state`, `/sequence`, etc.).
4. `host_tools/ros_web_dashboard.py` visualizes those topics.

## Quick Setup
From project root:

```bash
python3 -m venv rtes_env
source rtes_env/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install matplotlib
```

## Run
### 1) Flash firmware
```bash
idf.py build
idf.py -p /dev/ttyACM0 flash monitor
```

### 2) Start UDP -> ROS bridge (new terminal)
```bash
source /opt/ros/humble/setup.bash
source rtes_env/bin/activate
export RMW_IMPLEMENTATION=rmw_fastrtps_cpp
python3 host_tools/udp_to_ros2_bridge.py --bind-ip 0.0.0.0 --port 8888
```

### 3) Start web dashboard (new terminal)
```bash
source /opt/ros/humble/setup.bash
source rtes_env/bin/activate
export RMW_IMPLEMENTATION=rmw_fastrtps_cpp
python3 host_tools/ros_web_dashboard.py --host 127.0.0.1 --http-port 8080
```

Open `http://127.0.0.1:8080`.

## Verify ROS Data
```bash
source /opt/ros/humble/setup.bash
ros2 topic list
ros2 topic echo /distance
ros2 topic echo /parking_state
```