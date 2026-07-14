# raw_hailo_inference

**picam-raw UDP stream → Hailo NPU → JSON detections** — no OpenCV, no GStreamer, no FFmpeg.

```
picam-raw  (UDP raw YUV420, chunk protocol)
    │  UDP chunk reassembly
    │  chunk-0 extended header → timestamp_us, camera_index, camera_label
    ▼
TSQueue<RawFrame>   (drop-oldest)
    │
    ├─► letterbox YUV420 → RGB24  (hand-rolled, NEON-vectorisable)
    │
    ▼
Hailo-8 / 8L NPU   (HailoRT VDevice / VStreams)
    │  float32 output tensors  (YOLOv8 NMS)
    │
    ├─► TCP detection stream  (port 8558, newline-delimited JSON)
    └─► TCP status server     (port 8557, plain-text "status" command)
```

Output JSON per detection event (stdout + TCP broadcast):
```json
{"frame":N,"ts_us":T,"camera":{"index":0,"label":"cam0"},
 "detections":[{"class":"person","conf":0.92,
                "box":{"x0":0.1,"y0":0.2,"x1":0.4,"y1":0.8}}]}
```

`box` coordinates are normalised `[0,1]` against the **original source frame** — letterbox padding has already been removed. Multiply directly by the consumer's frame width/height.

Config parsed by `config.hpp` — hand-rolled INI parser (stdlib only).

---

## Dependencies

| Library | Purpose | Install |
|---|---|---|
| **HailoRT ≥ 4.18** | NPU inference | `.deb` from Hailo Developer Zone |

**No OpenCV. No GStreamer. No FFmpeg. No libav\*.**
Only HailoRT and POSIX sockets.

---

## Build

```bash
cmake -B build
cmake --build build -j$(nproc)
```

### From the APT repository

CI publishes to a signed APT repository (shared with other aipicam Raspberry Pi packages) hosted on Cloudflare R2, with two channels:

- **`main`** — pushing a `v*` tag publishes the clean release version here.
- **`nightly`** — every push (to any branch, and PRs) publishes a dev build here, versioned with a `+<UTC timestamp>` suffix.

```bash
curl -fsSL https://repo.aipicam.com/pubkey.asc | sudo gpg --dearmor -o /usr/share/keyrings/aipicam.gpg

# stable releases
echo "deb [signed-by=/usr/share/keyrings/aipicam.gpg] https://repo.aipicam.com main main" | sudo tee /etc/apt/sources.list.d/aipicam.list

# or nightly builds instead
echo "deb [signed-by=/usr/share/keyrings/aipicam.gpg] https://repo.aipicam.com nightly main" | sudo tee /etc/apt/sources.list.d/aipicam.list

sudo apt-get update
sudo apt-get install picam-hailo
```

Builds run on GitHub's native `ubuntu-24.04-arm` hosted runner (no QEMU), installing `hailort` from the Raspberry Pi apt repo (the same source `Dockerfile.native-arm64` uses — it's a plain public package there despite the Hailo Developer Zone requirement mentioned above). Uses the same `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `GPG_PRIVATE_KEY`, and `GPG_KEY_ID` repo secrets described in [pi-block-cpu-cores](../pi-block-cpu-cores)'s README, since it publishes into the same shared repo.

## Configure

Edit `config.ini`:
```ini
[hailo]
hef_path       = /usr/share/picam-hailo/models/yolov8m.hef
pipeline_depth = 8      ; inflight frames on NPU
fps_limit      = 30.0   ; max inference fps (0 = unlimited)

[input]
host           = 127.0.0.1
port           = 8561   ; 8561 = lores (640x360), 8560 = main
width          = 640
height         = 360
buffer_frames  = 4      ; TSQueue depth (drop-oldest)
ping_every     = 5      ; seconds between registration pings to picam-raw

[output]
ctrl_port      = 8557   ; status   : echo status | nc 127.0.0.1 8557
det_port       = 8558   ; stream   : nc 127.0.0.1 8558
```

## Run

```bash
./build/raw_hailo_inference                        # uses ./config.ini
./build/raw_hailo_inference --config /etc/hailo/config.ini

# Consume detections
nc 127.0.0.1 8558

# Check status
echo status | nc 127.0.0.1 8557
```

## Registration with picam-raw

On startup and every `ping_every` seconds, the process sends a UDP `HELLO` to picam-raw. picam-raw uses this to register the consumer and begin streaming YUV420 frames.

## Files

| File | Description |
|---|---|
| `config.ini` | Runtime configuration |
| `config.hpp` | Hand-rolled INI parser (stdlib only) |
| `raw_hailo_inference.cpp` | Pipeline: UDP receive → letterbox → infer → JSON detections |
| `CMakeLists.txt` | Build |
