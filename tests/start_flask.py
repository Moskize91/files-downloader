import threading
import traceback
from pathlib import Path
from typing import Iterator

from flask import Flask, Response, jsonify, request, send_file

app = Flask(__name__)
PORT = 3007

_MIN_LENGTH = 12500


@app.route("/images/<file_name>", methods=["GET", "HEAD"])
def get_image(file_name: str):
    image_path = Path(__file__).parent / "assets" / file_name
    if not image_path.exists():
        return "Image not found", 404

    image_file_size = image_path.stat().st_size
    enable_range = request.args.get("range", default=False, type=bool)
    reject_first = request.args.get("reject_first", default=False, type=bool)
    break_random = request.args.get("break_random", default=False, type=bool)
    no_content_length = request.args.get("no_content_length", default=False, type=bool)

    try:
        if request.method == "HEAD":
            response = send_file(image_path, mimetype="image/jpeg")
            if enable_range or not reject_first:
                response.headers["Accept-Ranges"] = "bytes"
                if no_content_length:
                    # 删除send_file自动添加的Content-Length头
                    response.headers.pop("Content-Length", None)
                else:
                    response.headers["Content-Length"] = str(image_file_size)

        elif request.method == "GET":
            range_header = request.headers.get(key="Range")
            headers: dict[str, str] = {}
            image_data: bytes | Iterator[bytes]
            if enable_range and range_header:
                start, end = _parse_range_header(range_header, image_file_size)
                length = end - start + 1
                should_break = break_random and length >= _MIN_LENGTH

                if should_break and _should_timeout():
                    return jsonify({"error": "Gateway Timeout"}), 504

                headers["Accept-Ranges"] = "bytes"
                headers["Content-Range"] = f"bytes {start}-{end}/{image_file_size}"
                headers["Content-Length"] = str(length)

                with open(image_path, "rb") as file:
                    file.seek(start)
                    image_data = file.read(length)

                if should_break:

                    def gen_image_data(data: bytes):
                        yield data[: length // 2]
                        raise ConnectionAbortedError("aborted")

                    image_data = gen_image_data(image_data)

            else:
                if not no_content_length:
                    headers["Content-Length"] = str(image_file_size)
                with open(image_path, "rb") as file:
                    image_data = file.read()

            response = Response(
                response=image_data,
                status=206 if enable_range and range_header else 200,
                headers=headers,
                mimetype="image/png",
            )
        else:
            raise RuntimeError("Unsupported method")
        return response

    except FileNotFoundError:
        return "Image not found", 404


@app.errorhandler(500)
def handle_500_error(_):
    return traceback.format_exc(), 500


def _parse_range_header(range_header: str, file_size: int):
    # Range header format: "bytes=start-end"
    if not range_header.startswith("bytes="):
        raise ValueError("Invalid range header format")

    range_part = range_header[6:]  # Remove "bytes=" prefix
    start, end = range_part.split("-")
    start = int(start) if start else 0
    end = int(end) if end else file_size - 1

    if start >= file_size or end >= file_size or start > end:
        raise ValueError("Invalid range")

    return start, end


_LOCK = threading.Lock()
_STEP: int = 0


def _should_timeout() -> bool:
    global _STEP  # pylint: disable=global-statement
    with _LOCK:
        _STEP += 1
        if _STEP % 2 == 0:
            return True
        return False


if __name__ == "__main__":
    app.run(debug=False, port=PORT)
