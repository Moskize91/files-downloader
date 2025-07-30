# DownloaderX 🚀

**Multi-threaded, resumable file downloader for Python**

DownloaderX is a high-performance Python library for downloading multiple files simultaneously with thread pooling and automatic resume support. Never lose progress on failed downloads again!

## Features ✨

- **Multi-threaded downloads** – Parallelize transfers for maximum speed
- **Resume broken downloads** – Auto-retries with checkpoint recovery
- **Progress tracking** – Real-time download stats and ETA
- **Simple API** – Just `download(url)` and you're done
- **Lightweight** – No heavy dependencies

## Installation

```bash
pip install downloaderx
```

## Why DownloaderX?

✔️ **Faster than requests/urllib** – Thread pooling squeezes your bandwidth
✔️ **Reliable** – Checksum verification (optional)
✔️ **Transparent** – Detailed logs and error reports