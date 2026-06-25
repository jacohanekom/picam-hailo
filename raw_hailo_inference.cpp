/**
 * raw_hailo_inference.cpp
 * =======================
 * Pipeline:
 *
 *   picam-raw UDP stream (raw YUV420)
 *       │  UDP chunk reassembly  (matches picam-raw chunk protocol exactly)
 *       │  chunk-0 extended header → timestamp_us, camera_index, camera_label
 *       ▼
 *   TSQueue<RawFrame>   (drop-oldest)
 *       │
 *       ├─► letterbox-resize YUV420 → RGB24  (hand-rolled, NEON-vectorisable)
 *       │
 *       ▼
 *   Hailo-8 / 8L NPU   (HailoRT VDevice / VStreams)
 *       │  float32 output tensors
 *       │
 *       ▼
 *   OutputWriter
 *       ├─ stdout  →  pipe to nc / redirect to file
 *       └─ TCP     →  connect to nc -lk <port>
 *
 * No OpenCV. No GStreamer. No FFmpeg. No libavcodec.
 * Only dependency beyond HailoRT is POSIX sockets.
 *
 * Build:   cmake -B build && cmake --build build -j$(nproc)
 * Run:     ./build/raw_hailo_inference [--config config.ini]
 *
 * Register with picam-raw:
 *   The receiver sends a UDP ping to picam-raw on startup and every
 *   ping_every seconds. picam-raw then starts sending frames.
 *
 * Output (stdout, newline-delimited JSON per detection event):
 *   {"frame":N,"frame_seq":S,"ts_us":T,"camera":{"index":I,"label":"L"},
 *    "detections":[{"class":"person","conf":0.92,
 *                   "box":{"x0":0.1,"y0":0.2,"x1":0.4,"y1":0.8}},...]}
 *
 *   box.x0/y0/x1/y1 are normalised [0,1] against the ORIGINAL SOURCE
 *   FRAME (the input.width x input.height configured above) — letterbox
 *   padding from the model's square input has already been removed.
 *   Downstream consumers can multiply directly by their own frame's
 *   width/height without needing any model-specific geometry.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

#include "config.hpp"
#include "hailo/hailort.hpp"

using namespace hailort;
using Clock = std::chrono::steady_clock;

// ─────────────────────────────────────────────────────────────────────────────
// Chunk header — must match picam-raw exactly
//
// Chunk 0: 32-byte extended header
//   [frame_seq:4][chunk_seq:2][total_chunks:2]
//   [timestamp_us:8][camera_index:1][camera_label:15]
//
// Chunk N>0: 8-byte header
//   [frame_seq:4][chunk_seq:2][total_chunks:2]
// ─────────────────────────────────────────────────────────────────────────────
static constexpr size_t kHeaderSize      = 8;
static constexpr size_t kChunk0HeaderSize = 32;
static constexpr size_t kLabelSize       = 15;

struct ChunkHeader {
    uint32_t frameSeq    = 0;
    uint16_t chunkSeq    = 0;
    uint16_t totalChunks = 0;
    int64_t  timestampUs = 0;   // only in chunk 0
    uint8_t  cameraIndex = 0;   // only in chunk 0
    char     cameraLabel[kLabelSize + 1] = {};  // only in chunk 0
};

static ChunkHeader parseHeader(const uint8_t* buf, size_t len) {
    ChunkHeader h;
    h.frameSeq    = (uint32_t(buf[0])<<24)|(uint32_t(buf[1])<<16)|
                    (uint32_t(buf[2])<<8) | uint32_t(buf[3]);
    h.chunkSeq    = (uint16_t(buf[4])<<8) | uint16_t(buf[5]);
    h.totalChunks = (uint16_t(buf[6])<<8) | uint16_t(buf[7]);
    if (h.chunkSeq == 0 && len >= kChunk0HeaderSize) {
        h.timestampUs =
            (int64_t(buf[ 8])<<56)|(int64_t(buf[ 9])<<48)|
            (int64_t(buf[10])<<40)|(int64_t(buf[11])<<32)|
            (int64_t(buf[12])<<24)|(int64_t(buf[13])<<16)|
            (int64_t(buf[14])<<8) | int64_t(buf[15]);
        h.cameraIndex = buf[16];
        std::memcpy(h.cameraLabel, &buf[17], kLabelSize);
        h.cameraLabel[kLabelSize] = '\0';
    }
    return h;
}

// ─────────────────────────────────────────────────────────────────────────────
// Global stop flag
// ─────────────────────────────────────────────────────────────────────────────
static std::atomic<bool> g_stop{false};
static void signal_handler(int) { g_stop = true; }

// ─────────────────────────────────────────────────────────────────────────────
// Per-stage latency tracker
// ─────────────────────────────────────────────────────────────────────────────
struct StageTimer {
    const char* name;
    double      accum_ms = 0;
    uint64_t    count    = 0;
    Clock::time_point t0 = Clock::now();
    void   start()   { t0 = Clock::now(); }
    void   stop()    { accum_ms += std::chrono::duration<double,std::milli>(
                           Clock::now()-t0).count(); ++count; }
    double avg_ms()  const { return count ? accum_ms/count : 0; }
    void   reset()   { accum_ms = 0; count = 0; }
};

// ─────────────────────────────────────────────────────────────────────────────
// RawFrame — packed YUV420, metadata from picam-raw chunk-0 header
// ─────────────────────────────────────────────────────────────────────────────
struct RawFrame {
    std::vector<uint8_t> data;       // packed YUV420: Y then U then V
    int      width       = 0;
    int      height      = 0;
    int64_t  timestampUs = 0;
    uint32_t frameSeq    = 0;
    uint8_t  cameraIndex = 0;
    char     cameraLabel[kLabelSize + 1] = {};
};

// ─────────────────────────────────────────────────────────────────────────────
// Thread-safe drop-oldest queue
// ─────────────────────────────────────────────────────────────────────────────
template<typename T>
class TSQueue {
public:
    explicit TSQueue(size_t cap = 4) : cap_(cap) {}

    void push(T v) {
        std::lock_guard<std::mutex> lk(mu_);
        while (q_.size() >= cap_) q_.pop();
        q_.push(std::move(v));
        cv_.notify_one();
    }

    bool pop(T& out, int ms = 100) {
        std::unique_lock<std::mutex> lk(mu_);
        if (!cv_.wait_for(lk, std::chrono::milliseconds(ms),
                          [&]{ return !q_.empty() || g_stop.load(); }))
            return false;
        if (q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop();
        return true;
    }

private:
    std::queue<T>           q_;
    std::mutex              mu_;
    std::condition_variable cv_;
    size_t                  cap_;
};

// ─────────────────────────────────────────────────────────────────────────────
// UdpRawReceiver
//
// Matches picam-raw's chunk protocol exactly.
// Sends a registration ping on start and every ping_every_s seconds so
// picam-raw keeps the client registered.
//
// Reassembles chunks into complete RawFrame objects and pushes them
// into the provided TSQueue.
// ─────────────────────────────────────────────────────────────────────────────
class UdpRawReceiver {
public:
    UdpRawReceiver(std::string host, int port,
                   int width, int height,
                   int pingEverySecs,
                   TSQueue<std::shared_ptr<RawFrame>>& q)
        : host_(std::move(host)), port_(port)
        , frameBytes_(static_cast<size_t>(width * height * 3 / 2))
        , width_(width), height_(height)
        , pingEvery_(pingEverySecs), q_(q)
    {}

    ~UdpRawReceiver() {
        if (sock_ >= 0) ::close(sock_);
        if (recvThread_.joinable()) recvThread_.join();
        if (pingThread_.joinable()) pingThread_.join();
    }

    void start() {
        sock_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (sock_ < 0) throw std::runtime_error("UdpRawReceiver: socket() failed");

        // Large receive buffer to handle burst of chunks
        int rcvbuf = 64 * 1024 * 1024;
        ::setsockopt(sock_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
        socklen_t optlen = sizeof(rcvbuf);
        ::getsockopt(sock_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, &optlen);
        std::cerr << "[UDP] Receive buffer: " << rcvbuf / 1024 << " KB\n";

        sockaddr_in local{};
        local.sin_family      = AF_INET;
        local.sin_addr.s_addr = INADDR_ANY;
        local.sin_port        = 0;
        if (::bind(sock_, reinterpret_cast<sockaddr*>(&local), sizeof(local)) < 0)
            throw std::runtime_error("UdpRawReceiver: bind() failed");

        std::memset(&server_, 0, sizeof(server_));
        server_.sin_family = AF_INET;
        server_.sin_port   = htons(static_cast<uint16_t>(port_));
        if (::inet_pton(AF_INET, host_.c_str(), &server_.sin_addr) != 1)
            throw std::runtime_error("UdpRawReceiver: invalid host: " + host_);

        pingThread_ = std::thread(&UdpRawReceiver::pingLoop, this);
        recvThread_ = std::thread(&UdpRawReceiver::recvLoop, this);

        std::cerr << "[UDP] Receiving " << width_ << "x" << height_
                  << " YUV420 from " << host_ << ":" << port_ << "\n";
    }

    bool streamReady() const { return framesReceived_.load() > 0; }

    bool waitForStream(int timeoutSecs) {
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(timeoutSecs);
        while (std::chrono::steady_clock::now() < deadline && !g_stop) {
            if (streamReady()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return streamReady();
    }

private:
    void pingLoop() {
        while (!g_stop) {
            ::sendto(sock_, "HELLO", 5, 0,
                     reinterpret_cast<sockaddr*>(&server_), sizeof(server_));
            for (int i = 0; i < pingEvery_ * 10 && !g_stop; ++i)
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    void recvLoop() {
        std::vector<uint8_t> buf(kChunk0HeaderSize + 65536);

        std::map<uint32_t, std::map<uint16_t, std::vector<uint8_t>>> partial;
        std::map<uint32_t, ChunkHeader> frameMeta;

        while (!g_stop) {
            ssize_t n = ::recv(sock_, buf.data(), buf.size(), 0);
            if (n < 0) { if (errno == EINTR || g_stop) break; continue; }
            if (static_cast<size_t>(n) < kHeaderSize) continue;

            ChunkHeader hdr = parseHeader(buf.data(), static_cast<size_t>(n));
            if (hdr.totalChunks == 0) continue;

            size_t hdrSize = (hdr.chunkSeq == 0) ? kChunk0HeaderSize : kHeaderSize;
            if (static_cast<size_t>(n) <= hdrSize) continue;

            // Evict oldest partial frame if too many in flight
            if (partial.size() > 32) {
                auto it = partial.begin();
                frameMeta.erase(it->first);
                partial.erase(it);
            }

            if (hdr.chunkSeq == 0)
                frameMeta[hdr.frameSeq] = hdr;

            partial[hdr.frameSeq][hdr.chunkSeq].assign(
                buf.begin() + hdrSize, buf.begin() + n);

            auto& chunks = partial[hdr.frameSeq];
            if (static_cast<uint16_t>(chunks.size()) != hdr.totalChunks)
                continue;

            // All chunks received — reassemble
            std::vector<uint8_t> frameData;
            frameData.reserve(frameBytes_);
            bool ok = true;
            for (uint16_t i = 0; i < hdr.totalChunks; ++i) {
                auto it = chunks.find(i);
                if (it == chunks.end()) { ok = false; break; }
                frameData.insert(frameData.end(),
                                 it->second.begin(), it->second.end());
            }

            partial.erase(hdr.frameSeq);

            if (!ok || frameData.size() < frameBytes_) {
                frameMeta.erase(hdr.frameSeq);
                continue;
            }
            frameData.resize(frameBytes_);

            auto rf = std::make_shared<RawFrame>();
            rf->data   = std::move(frameData);
            rf->width  = width_;
            rf->height = height_;

            rf->frameSeq = hdr.frameSeq;
            auto mit = frameMeta.find(hdr.frameSeq);
            if (mit != frameMeta.end()) {
                rf->timestampUs = mit->second.timestampUs;
                rf->cameraIndex = mit->second.cameraIndex;
                std::memcpy(rf->cameraLabel, mit->second.cameraLabel,
                            kLabelSize + 1);
                frameMeta.erase(mit);
            } else {
                rf->timestampUs = std::chrono::duration_cast<
                    std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
            }

            q_.push(std::move(rf));
            ++framesReceived_;
        }
    }

    std::string   host_;
    int           port_;
    size_t        frameBytes_;
    int           width_, height_, pingEvery_;
    TSQueue<std::shared_ptr<RawFrame>>& q_;
    int           sock_ = -1;
    sockaddr_in   server_{};
    std::thread   recvThread_;
    std::thread   pingThread_;
    std::atomic<int> framesReceived_{0};
};

// ─────────────────────────────────────────────────────────────────────────────
// YUV420 → RGB24 conversion (BT.601 limited-range, NEON auto-vectorisable)
// Packed YUV420 input: Y plane (width*height), U plane, V plane.
// dst_stride: output row stride in bytes. Pass w*3 for packed,
//             or canvas_w*3 to write into a letterbox canvas.
// ─────────────────────────────────────────────────────────────────────────────
static inline uint8_t yuv_clip(int v) {
    return v < 0 ? 0 : v > 255 ? 255 : static_cast<uint8_t>(v);
}

__attribute__((optimize("O3,tree-vectorize")))
static void yuv420_packed_to_rgb24(
        const uint8_t* src,     // packed YUV420: Y then U then V
        int            width,
        int            height,
        uint8_t*       dst,     // output RGB24
        int            dst_stride,  // output row stride in bytes
        // letterbox: write into sub-region of canvas
        int            src_w,   // source width (may differ for scale)
        int            src_h)   // source height
{
    const uint8_t* sy = src;
    const uint8_t* su = src + width * height;
    const uint8_t* sv = su  + (width / 2) * (height / 2);

    const int sfx = (src_w << 16) / width;
    const int sfy = (src_h << 16) / height;

    bool no_scale = (src_w == width && src_h == height);

    for (int dy = 0; dy < height; ++dy) {
        int sy_c = no_scale ? dy : std::min((dy * sfy) >> 16, src_h - 1);
        const uint8_t* ry = sy + sy_c * src_w;
        const uint8_t* ru = su + (sy_c >> 1) * (src_w / 2);
        const uint8_t* rv = sv + (sy_c >> 1) * (src_w / 2);
        uint8_t* rd = dst + dy * dst_stride;
        for (int dx = 0; dx < width; ++dx) {
            int sx_c = no_scale ? dx : std::min((dx * sfx) >> 16, src_w - 1);
            int C = 298 * ((int)ry[sx_c] - 16) + 128;
            int U = (int)ru[sx_c >> 1] - 128;
            int V = (int)rv[sx_c >> 1] - 128;
            rd[dx*3+0] = yuv_clip((C         + 409*V) >> 8); // R
            rd[dx*3+1] = yuv_clip((C - 100*U - 208*V) >> 8); // G
            rd[dx*3+2] = yuv_clip((C + 516*U         ) >> 8); // B
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared inference status — written by main loop, read by StatusServer
// ─────────────────────────────────────────────────────────────────────────────
struct InferenceStatus {
    std::mutex   mu;
    uint64_t     frame_count    = 0;
    uint64_t     det_count      = 0;
    float        fps            = 0.0f;
    float        avg_fps        = 0.0f;
    float        hailo_temp     = -1.0f;
    float        ms_wait        = 0.0f;
    float        ms_pre         = 0.0f;
    float        ms_write       = 0.0f;
    float        ms_npu         = 0.0f;
    std::string  last_detection; // last non-empty JSON line
    std::string  model;
    int          input_width    = 0;
    int          input_height   = 0;
    std::string  input_host;
    int          input_port     = 0;
};
static InferenceStatus g_status;

// ─────────────────────────────────────────────────────────────────────────────
// StatusServer — TCP plain-text on ctrl_port
// Commands: status
// ─────────────────────────────────────────────────────────────────────────────
class StatusServer {
public:
    StatusServer(int port) : port_(port) {}
    ~StatusServer() { if (fd_ >= 0) ::close(fd_); }

    void start() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(static_cast<uint16_t>(port_));
        ::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(fd_, 8);
        thread_ = std::thread(&StatusServer::acceptLoop, this);
        thread_.detach();
        std::cerr << "[Status] Listening on 0.0.0.0:" << port_ << "\n";
    }

private:
    void acceptLoop() {
        while (!g_stop) {
            int cfd = ::accept(fd_, nullptr, nullptr);
            if (cfd < 0) { if (errno == EINTR) continue; break; }
            std::thread([cfd]{ handle(cfd); }).detach();
        }
    }

    static void handle(int cfd) {
        struct timeval tv{2, 0};
        ::setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        std::string line;
        char c;
        while (::recv(cfd, &c, 1, 0) == 1) {
            if (c == '\n') break;
            if (c != '\r') line += c;
        }

        std::string reply;
        if (line == "status" || line.empty()) {
            std::lock_guard<std::mutex> lk(g_status.mu);
            char buf[1024];
            snprintf(buf, sizeof(buf),
                "ok=true\n"
                "frame=%llu\n"
                "detections=%llu\n"
                "fps=%.1f\n"
                "avg_fps=%.1f\n"
                "hailo_temp=%.1f\n"
                "ms_wait=%.1f\n"
                "ms_pre=%.1f\n"
                "ms_write=%.1f\n"
                "ms_npu=%.1f\n"
                "model=%s\n"
                "input=%s:%d  %dx%d\n"
                "\n",
                (unsigned long long)g_status.frame_count,
                (unsigned long long)g_status.det_count,
                g_status.fps, g_status.avg_fps,
                g_status.hailo_temp,
                g_status.ms_wait, g_status.ms_pre,
                g_status.ms_write, g_status.ms_npu,
                g_status.model.c_str(),
                g_status.input_host.c_str(), g_status.input_port,
                g_status.input_width, g_status.input_height);
            reply = buf;
        } else {
            reply = "ok=false\nerror=unknown command\ncommands=status\n\n";
        }
        ::send(cfd, reply.data(), reply.size(), MSG_NOSIGNAL);
        ::close(cfd);
    }

    int         port_;
    int         fd_ = -1;
    std::thread thread_;
};

// ─────────────────────────────────────────────────────────────────────────────
// DetectionServer — TCP server that broadcasts detection JSON to all clients
//
// Clients connect and receive a newline-delimited JSON stream.
// Clients that can't keep up are dropped (non-blocking send).
//
// Usage:
//   nc 127.0.0.1 8558
//   Or in Python: socket.connect(('127.0.0.1', 8558))
// ─────────────────────────────────────────────────────────────────────────────
class DetectionServer {
public:
    DetectionServer(int port) : port_(port) {}

    ~DetectionServer() {
        if (listenFd_ >= 0) ::close(listenFd_);
    }

    void start() {
        listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        ::setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(static_cast<uint16_t>(port_));
        ::bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listenFd_, 16);
        thread_ = std::thread(&DetectionServer::acceptLoop, this);
        thread_.detach();
        std::cerr << "[Detections] Listening on 0.0.0.0:" << port_ << "\n";
    }

    // Broadcast a detection JSON line to all connected clients.
    // Called from the main inference loop — must not block.
    void broadcast(const std::string& line) {
        std::string msg = line + "\n";
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<int> alive;
        for (int fd : clients_) {
            ssize_t n = ::send(fd, msg.data(), msg.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
            if (n > 0)
                alive.push_back(fd);
            else
                ::close(fd);  // slow or disconnected client — drop it
        }
        clients_ = std::move(alive);
    }

    int clientCount() const {
        std::lock_guard<std::mutex> lk(mu_);
        return static_cast<int>(clients_.size());
    }

private:
    void acceptLoop() {
        while (!g_stop) {
            sockaddr_in peer{};
            socklen_t   plen = sizeof(peer);
            int cfd = ::accept(listenFd_,
                               reinterpret_cast<sockaddr*>(&peer), &plen);
            if (cfd < 0) { if (errno == EINTR) continue; break; }

            char ipbuf[INET_ADDRSTRLEN];
            ::inet_ntop(AF_INET, &peer.sin_addr, ipbuf, sizeof(ipbuf));
            std::cerr << "\n[Detections] Client connected: "
                      << ipbuf << ":" << ntohs(peer.sin_port) << "\n";

            std::lock_guard<std::mutex> lk(mu_);
            clients_.push_back(cfd);
        }
    }

    int                  port_;
    int                  listenFd_ = -1;
    std::thread          thread_;
    mutable std::mutex   mu_;
    std::vector<int>     clients_;
};

struct Det { int cls; float conf, x0, y0, x1, y1; };

static constexpr float CONF_THRESHOLD = 0.5f;
static constexpr int   MAX_DETS       = 100;
static constexpr int   N_CLASSES      = 80;

static const char* COCO_NAMES[80] = {
    "person","bicycle","car","motorcycle","airplane","bus","train",
    "truck","boat","traffic light","fire hydrant","stop sign",
    "parking meter","bench","bird","cat","dog","horse","sheep","cow",
    "elephant","bear","zebra","giraffe","backpack","umbrella",
    "handbag","tie","suitcase","frisbee","skis","snowboard",
    "sports ball","kite","baseball bat","baseball glove","skateboard",
    "surfboard","tennis racket","bottle","wine glass","cup","fork",
    "knife","spoon","bowl","banana","apple","sandwich","orange",
    "broccoli","carrot","hot dog","pizza","donut","cake","chair",
    "couch","potted plant","bed","dining table","toilet","tv",
    "laptop","mouse","remote","keyboard","cell phone","microwave",
    "oven","toaster","sink","refrigerator","book","clock","vase",
    "scissors","teddy bear","hair drier","toothbrush"
};

static std::string format_detections(
        uint64_t frame_count,
        uint32_t frame_seq,
        int64_t  timestamp_us,
        uint8_t  camera_index,
        const char* camera_label,
        const std::vector<std::vector<float>>& obufs,
        int      imgW,
        int      imgH,
        int      lb_ox, int lb_oy,
        int      lb_cw, int lb_ch)
{
    // Decode YOLOv8 NMS output
    std::vector<Det> dets;

    if (!obufs.empty()) {
        const auto& buf = obufs[0];
        const int class_stride = 1 + MAX_DETS * 5;
        for (int c = 0; c < N_CLASSES; ++c) {
            size_t base = static_cast<size_t>(c * class_stride);
            if (base >= buf.size()) break;
            int n = static_cast<int>(buf[base]);
            for (int d = 0; d < n; ++d) {
                size_t off = base + 1 + static_cast<size_t>(d * 5);
                if (off + 4 >= buf.size()) break;
                float conf = buf[off + 4];
                if (conf < CONF_THRESHOLD) continue;
                dets.push_back({c, conf,
                    buf[off+1], buf[off+0],
                    buf[off+3], buf[off+2]});
            }
        }
    }

    // Map each detection from model-input space (normalised [0,1] against
    // the full padded mw×mh canvas) to [0,1] normalised against the
    // ORIGINAL SOURCE FRAME content region (padding removed). This is what
    // downstream consumers (picam-webrtc, save_events.py, etc.) need —
    // coordinates relative to the actual camera frame, not the model's
    // square/padded input.
    //
    //   model_px = d.x0 * mw                 (pixel position in model canvas)
    //   content_px = model_px - lb_ox        (pixel position within content region)
    //   normalised = content_px / lb_cw      (back to [0,1], relative to source frame)
    for (auto& d : dets) {
        float model_x0 = d.x0 * static_cast<float>(imgW);
        float model_x1 = d.x1 * static_cast<float>(imgW);
        float model_y0 = d.y0 * static_cast<float>(imgH);
        float model_y1 = d.y1 * static_cast<float>(imgH);

        d.x0 = std::clamp((model_x0 - lb_ox) / static_cast<float>(lb_cw), 0.0f, 1.0f);
        d.x1 = std::clamp((model_x1 - lb_ox) / static_cast<float>(lb_cw), 0.0f, 1.0f);
        d.y0 = std::clamp((model_y0 - lb_oy) / static_cast<float>(lb_ch), 0.0f, 1.0f);
        d.y1 = std::clamp((model_y1 - lb_oy) / static_cast<float>(lb_ch), 0.0f, 1.0f);
    }

    if (dets.empty()) return {};

    std::string j;
    j.reserve(512 + dets.size() * 128);
    j += "{\"frame\":";     j += std::to_string(frame_count);
    j += ",\"frame_seq\":"; j += std::to_string(frame_seq);
    j += ",\"ts_us\":";     j += std::to_string(timestamp_us);
    j += ",\"camera\":{\"index\":";
    j += std::to_string(static_cast<int>(camera_index));
    j += ",\"label\":\"";  j += camera_label; j += "\"}";
    j += ",\"detections\":[";
    for (size_t i = 0; i < dets.size(); ++i) {
        const auto& d = dets[i];
        if (i) j += ',';
        char tmp[256];
        snprintf(tmp, sizeof(tmp),
                 "{\"class\":\"%s\",\"conf\":%.4f,"
                 "\"box\":{\"x0\":%.4f,\"y0\":%.4f,\"x1\":%.4f,\"y1\":%.4f}}",
                 COCO_NAMES[d.cls], d.conf, d.x0, d.y0, d.x1, d.y1);
        j += tmp;
    }
    j += "]}";
    return j;
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    setenv("HAILORT_LOGGER_PATH", "NONE", 1);
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGHUP,  signal_handler);
    std::signal(SIGPIPE, SIG_IGN);

    // Pin to CPU core 3
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(3, &cpuset);
        if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0)
            std::cerr << "[Main] Warning: failed to set CPU affinity\n";
        else
            std::cerr << "[Main] Pinned to CPU core 3\n";
    }

    std::string cfg_path = "config.ini";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if ((a == "--config" || a == "-c") && i + 1 < argc) cfg_path = argv[++i];
        else if (a == "--help" || a == "-h") {
            std::cout << "Usage: " << argv[0] << " [--config config.ini]\n";
            return 0;
        }
    }

    Config cfg(cfg_path);

    const std::string hef_path      = cfg.get_str("hailo.hef_path");
    const int         pipeline_depth= cfg.get_int("hailo.pipeline_depth", 4);
    const double      fps_limit     = cfg.get_double("hailo.fps_limit", 30.0);
    const std::string host          = cfg.get_str("input.host", "127.0.0.1");
    const int         port          = cfg.get_int("input.port", 8561);
    const int         width         = cfg.get_int("input.width", 640);
    const int         height        = cfg.get_int("input.height", 360);
    const int         q_depth       = cfg.get_int("input.buffer_frames", 4);
    const int         ping_every    = cfg.get_int("input.ping_every", 5);
    const int         ctrl_port     = cfg.get_int("output.ctrl_port", 8557);
    const int         det_port      = cfg.get_int("output.det_port",  8558);

    if (hef_path.empty()) { std::cerr << "hailo.hef_path not set\n"; return 1; }

    // Populate static status fields
    {
        std::lock_guard<std::mutex> lk(g_status.mu);
        g_status.model        = hef_path;
        g_status.input_host   = host;
        g_status.input_port   = port;
        g_status.input_width  = width;
        g_status.input_height = height;
    }

    std::cerr << "[Config] input   : udp://" << host << ":" << port
              << "  " << width << "x" << height << "\n"
              << "[Config] hef     : " << hef_path << "\n"
              << "[Config] status  : 0.0.0.0:" << ctrl_port  << "\n"
              << "[Config] detects : 0.0.0.0:" << det_port   << "\n";

    // ── Load HEF ─────────────────────────────────────────────────────────────
    auto hef_res = Hef::create(hef_path);
    if (!hef_res) { std::cerr << "[Hailo] Cannot load HEF\n"; return 1; }
    Hef hef = hef_res.release();

    // ── Hailo state ───────────────────────────────────────────────────────────
    std::unique_ptr<VDevice> vdevice;
    std::vector<std::shared_ptr<ConfiguredNetworkGroup>> network_groups;
    std::vector<InputVStream>  ivs;
    std::vector<OutputVStream> ovs;
    std::unique_ptr<ActivatedNetworkGroup> activated;
    std::vector<std::vector<float>> obufs;
    int    mw = 0, mh = 0;
    size_t ibytes = 0;

    auto init_hailo = [&]() -> bool {
        std::cerr << "[Hailo] Initialising...\n";
        hailo_vdevice_params_t vparams{};
        hailo_init_vdevice_params(&vparams);
        vparams.scheduling_algorithm = HAILO_SCHEDULING_ALGORITHM_NONE;
        auto vd = VDevice::create(vparams);
        if (!vd) { std::cerr << "[Hailo] VDevice failed\n"; return false; }
        vdevice = vd.release();

        auto cfgp = hef.create_configure_params(HAILO_STREAM_INTERFACE_PCIE);
        if (!cfgp) { std::cerr << "[Hailo] configure_params failed\n"; return false; }
        auto ngr = vdevice->configure(hef, cfgp.value());
        if (!ngr || ngr->empty()) { std::cerr << "[Hailo] configure failed\n"; return false; }
        network_groups = ngr.release();
        auto& ng = network_groups[0];

        auto inp  = ng->make_input_vstream_params(false, HAILO_FORMAT_TYPE_UINT8,
                        HAILO_DEFAULT_VSTREAM_TIMEOUT_MS, pipeline_depth);
        auto outp = ng->make_output_vstream_params(false, HAILO_FORMAT_TYPE_FLOAT32,
                        HAILO_DEFAULT_VSTREAM_TIMEOUT_MS, pipeline_depth);
        if (!inp || !outp) { std::cerr << "[Hailo] vstream params failed\n"; return false; }

        auto ivsr = VStreamsBuilder::create_input_vstreams(*ng, inp.value());
        auto ovsr = VStreamsBuilder::create_output_vstreams(*ng, outp.value());
        if (!ivsr || !ovsr) { std::cerr << "[Hailo] vstreams failed\n"; return false; }
        ivs = ivsr.release();
        ovs = ovsr.release();

        auto info = ivs[0].get_info();
        mw = static_cast<int>(info.shape.width);
        mh = static_cast<int>(info.shape.height);
        ibytes = ivs[0].get_frame_size();

        std::cerr << "[Hailo] model input: " << mw << "x" << mh
                  << "  pipeline_depth=" << pipeline_depth << "\n";

        auto act = ng->activate();
        if (!act) { std::cerr << "[Hailo] activate failed\n"; return false; }
        activated = act.release();

        obufs.resize(ovs.size());
        for (size_t i = 0; i < ovs.size(); ++i)
            obufs[i].resize(ovs[i].get_frame_size() / sizeof(float));

        std::cerr << "[Hailo] Ready. " << ovs.size() << " output vstream(s)\n";
        return true;
    };

    // ── Preprocess / letterbox state ──────────────────────────────────────────
    std::vector<std::vector<uint8_t>> ibuf_pool;
    struct InflightFrame { int slot; std::shared_ptr<RawFrame> frame; };
    TSQueue<InflightFrame> inflight(pipeline_depth * 2);

    // Compute letterbox geometry: scale src into mw×mh preserving aspect ratio
    int lb_ox = 0, lb_oy = 0; // offset of content in canvas
    int lb_cw = 0, lb_ch = 0; // content dimensions in canvas
    auto rebuild_letterbox = [&](int sw, int sh) {
        float sc = std::min(static_cast<float>(mw) / sw,
                            static_cast<float>(mh) / sh);
        lb_cw = static_cast<int>(sw * sc);
        lb_ch = static_cast<int>(sh * sc);
        lb_ox = (mw - lb_cw) / 2;
        lb_oy = (mh - lb_ch) / 2;
        for (auto& buf : ibuf_pool) std::fill(buf.begin(), buf.end(), 114);
        if (lb_ox == 0 && lb_oy == 0)
            std::cerr << "[Main] Direct fit: " << sw << "x" << sh
                      << " == model " << mw << "x" << mh << "\n";
        else
            std::cerr << "[Main] Letterbox: " << sw << "x" << sh
                      << " → content " << lb_cw << "x" << lb_ch
                      << " + padding " << lb_ox << "x" << lb_oy
                      << " in " << mw << "x" << mh << "\n";
    };

    // ── Timers ────────────────────────────────────────────────────────────────
    StageTimer tm_pre{"preprocess"}, tm_write{"hailo_write"},
               tm_read{"hailo_read"}, tm_json{"json+send"},
               tm_pop{"frame_wait "};

    // ── Detection server ──────────────────────────────────────────────────────
    DetectionServer det_srv(det_port);
    det_srv.start();

    // ── Status server ─────────────────────────────────────────────────────────
    StatusServer status_srv(ctrl_port);
    status_srv.start();

    // ── UDP receiver ──────────────────────────────────────────────────────────
    TSQueue<std::shared_ptr<RawFrame>> fq(q_depth);
    UdpRawReceiver receiver(host, port, width, height, ping_every, fq);
    receiver.start();

    // ── Wait for first frame → init Hailo ────────────────────────────────────
    std::cerr << "[Main] Waiting for first frame from picam-raw...\n";
    {
        std::shared_ptr<RawFrame> first;
        while (!g_stop && !fq.pop(first, 100));
        if (g_stop) return 0;
        if (!init_hailo()) return 1;
        ibuf_pool.assign(static_cast<size_t>(pipeline_depth),
                         std::vector<uint8_t>(ibytes, 114));
        rebuild_letterbox(first->width, first->height);
        fq.push(first);
        std::cerr << "[Main] First frame: " << first->width
                  << "x" << first->height << "\n";
    }

    // ── FPS limiter ───────────────────────────────────────────────────────────
    const int64_t frame_interval_us =
        fps_limit > 0 ? static_cast<int64_t>(1e6 / fps_limit) : 0;
    auto last_frame_time = Clock::now();

    // ── Writer thread: fq → preprocess → NPU write ───────────────────────────
    std::thread writer_thread([&]() {
        int wslot = 0;
        while (!g_stop) {
            std::shared_ptr<RawFrame> fp;
            tm_pop.start();
            if (!fq.pop(fp, 100)) { tm_pop.stop(); continue; }
            tm_pop.stop();

            // FPS limiter — drop frames to stay within fps_limit
            if (frame_interval_us > 0) {
                auto now = Clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                    now - last_frame_time).count();
                if (elapsed < frame_interval_us) continue;
                last_frame_time = now;
            }

            tm_pre.start();
            {
                auto& ibuf = ibuf_pool[static_cast<size_t>(wslot)];
                // Write content into letterbox canvas at (lb_ox, lb_oy)
                uint8_t* content = ibuf.data() +
                    (static_cast<size_t>(lb_oy) * mw + lb_ox) * 3;
                yuv420_packed_to_rgb24(
                    fp->data.data(), lb_cw, lb_ch,
                    content, mw * 3,
                    fp->width, fp->height);
            }
            tm_pre.stop();

            tm_write.start();
            {
                auto& ibuf = ibuf_pool[static_cast<size_t>(wslot)];
                hailo_status ws = HAILO_TIMEOUT;
                while (ws == HAILO_TIMEOUT && !g_stop)
                    ws = ivs[0].write(MemoryView(ibuf.data(), ibuf.size()));
                if (g_stop) break;
                if (ws != HAILO_SUCCESS) {
                    std::cerr << "[Writer] Hailo write error\n"; continue;
                }
            }
            tm_write.stop();

            inflight.push({wslot, fp});
            wslot = (wslot + 1) % pipeline_depth;
        }
    });

    // ── Main thread: NPU read → JSON → output ────────────────────────────────
    std::cerr << "[Main] Running.\n"
              << "[Main]   Detections : nc 127.0.0.1 " << det_port   << "\n"
              << "[Main]   Status     : echo status | nc 127.0.0.1 " << ctrl_port  << "\n";

    uint64_t frame_count = 0;
    auto     t_start     = Clock::now();

    while (!g_stop) {
        InflightFrame inf;
        if (!inflight.pop(inf, 200)) continue;

        tm_read.start();
        bool ok = true;
        for (size_t i = 0; i < ovs.size() && !g_stop; ++i) {
            hailo_status rs = HAILO_TIMEOUT;
            while (rs == HAILO_TIMEOUT && !g_stop)
                rs = ovs[i].read(MemoryView(obufs[i].data(),
                                             obufs[i].size() * sizeof(float)));
            if (rs != HAILO_SUCCESS) { ok = false; break; }
        }
        tm_read.stop();
        if (g_stop || !ok) continue;

        ++frame_count;

        std::fill(ibuf_pool[static_cast<size_t>(inf.slot)].begin(),
                  ibuf_pool[static_cast<size_t>(inf.slot)].end(), 114);

        tm_json.start();
        std::string json = format_detections(
            frame_count,
            inf.frame->frameSeq,
            inf.frame->timestampUs,
            inf.frame->cameraIndex,
            inf.frame->cameraLabel,
            obufs,
            mw, mh,
            lb_ox, lb_oy, lb_cw, lb_ch);
        if (!json.empty())
            det_srv.broadcast(json);
        tm_json.stop();

        // ── Console stats (stderr, 1Hz, single overwriting line) ─────────────
        {
            static auto     t_last         = Clock::now();
            static uint64_t frames_since   = 0;
            static uint64_t dets_since     = 0;
            ++frames_since;
            if (!json.empty()) ++dets_since;

            auto   now   = Clock::now();
            double since = std::chrono::duration<double>(now - t_last).count();
            if (since >= 1.0) {
                double fps     = frames_since / since;
                double avg_fps = frame_count /
                    std::chrono::duration<double>(now - t_start).count();

                // Read Hailo temperature via HailoRT API (4.20.0)
                float hailo_temp = -1.0f;
                {
                    auto devices_res = vdevice->get_physical_devices();
                    if (devices_res) {
                        for (auto& dev_ref : devices_res.value()) {
                            auto temp = dev_ref.get().get_chip_temperature();
                            if (temp) {
                                hailo_temp = temp->ts0_temperature;
                                break;
                            }
                        }
                    }
                }

                // Update shared status
                {
                    std::lock_guard<std::mutex> lk(g_status.mu);
                    g_status.frame_count = frame_count;
                    g_status.fps         = static_cast<float>(fps);
                    g_status.avg_fps     = static_cast<float>(avg_fps);
                    g_status.hailo_temp  = hailo_temp;
                    g_status.ms_wait     = static_cast<float>(tm_pop.avg_ms());
                    g_status.ms_pre      = static_cast<float>(tm_pre.avg_ms());
                    g_status.ms_write    = static_cast<float>(tm_write.avg_ms());
                    g_status.ms_npu      = static_cast<float>(tm_read.avg_ms());
                    if (!json.empty()) {
                        g_status.det_count++;
                        g_status.last_detection = json;
                    }
                }

                if (hailo_temp >= 0)
                    fprintf(stderr,
                        "\r[%6llu] fps:%4.1f avg:%4.1f  det/s:%-3llu"
                        "  wait:%.1fms pre:%.1fms wri:%.1fms npu:%.1fms  hailo:%.1f°C   ",
                        (unsigned long long)frame_count, fps, avg_fps,
                        (unsigned long long)dets_since,
                        tm_pop.avg_ms(), tm_pre.avg_ms(),
                        tm_write.avg_ms(), tm_read.avg_ms(), hailo_temp);
                else
                    fprintf(stderr,
                        "\r[%6llu] fps:%4.1f avg:%4.1f  det/s:%-3llu"
                        "  wait:%.1fms pre:%.1fms wri:%.1fms npu:%.1fms             ",
                        (unsigned long long)frame_count, fps, avg_fps,
                        (unsigned long long)dets_since,
                        tm_pop.avg_ms(), tm_pre.avg_ms(),
                        tm_write.avg_ms(), tm_read.avg_ms());
                fflush(stderr);

                t_last       = now;
                frames_since = 0;
                dets_since   = 0;
                tm_pop.reset(); tm_pre.reset();
                tm_write.reset(); tm_read.reset(); tm_json.reset();
            }
        }
    }

    fprintf(stderr, "\n");

    // ── Shutdown ──────────────────────────────────────────────────────────────
    g_stop = true;
    writer_thread.join();
    if (activated)      activated.reset();
    network_groups.clear();
    if (vdevice)        vdevice.reset();

    double total = std::chrono::duration<double>(Clock::now() - t_start).count();
    std::cerr << "[Main] Done. frames=" << frame_count
              << "  avg_fps=" << frame_count / total << "\n";
    return 0;
}
