import traceback

from pathlib import Path
from flask import request, send_file, Flask, Response


app = Flask(__name__)
PORT = 3007

@app.route("/images/mirai.jpg", methods=["GET", "HEAD"])
def get_image():
  image_path = Path(__file__).parent / "mirai.jpg"
  image_file_size = image_path.stat().st_size
  enable_range = request.args.get("range", default=False, type=bool)
  reject_first = request.args.get("reject_first", default=False, type=bool)

  try:
    if request.method == "HEAD":
      response = send_file(image_path, mimetype="image/jpeg")
      if enable_range or not reject_first:
        response.headers["Accept-Ranges"] = "bytes"
        response.headers["Content-Length"] = str(image_file_size)

    elif request.method == "GET":
      range_header = request.headers.get(key="Range")
      headers: dict[str, str] = {}
      image_data: bytes
      if enable_range and range_header:
        start, end = _parse_range_header(range_header, image_file_size)
        length = end - start + 1
        headers["Accept-Ranges"] = "bytes"
        headers["Content-Range"] = f"bytes {start}-{end}/{end - start + 1}"
        headers["Content-Length"] = str(length)
        with open(image_path, "rb") as file:
          file.seek(start)
          image_data = file.read(length)
      else:
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
def handle_500_error(e):
  return traceback.format_exc(), 500

def _parse_range_header(range_header: str, file_size: int):
  start, end = range_header.split("-")
  start = int(start) if start else 0
  end = int(end) if end else file_size - 1

  if start >= file_size or end >= file_size or start > end:
    raise ValueError("Invalid range")

  return start, end

if __name__ == "__main__":
  app.run(debug=False, port=PORT)