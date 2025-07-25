from pathlib import Path
from flask import request, send_file, Flask, Response


app = Flask(__name__)

@app.route("/images/mirai.jpg", methods=["GET", "HEAD"])
def get_image():
  image_path = Path(__file__).parent / "mirai.jpg"
  enable_range = request.args.get("range", default=False, type=bool)
  reject_first = request.args.get("reject_first", default=False, type=bool)

  try:
    if request.method == "HEAD":
      response = send_file(image_path, mimetype="image/jpeg")
      if enable_range or not reject_first:
        response.headers["Accept-Ranges"] = "bytes"
        response.headers["Content-Length"] = str(image_path.stat().st_size)

    elif request.method == "GET":
      if enable_range:
        response = send_file(image_path, mimetype="image/jpeg")
      else:
        with open(image_path, "rb") as f:
          image_data = f.read()
        response = Response(
          response=image_data,
          status=200,
          mimetype="image/png",
        )
    else:
      raise RuntimeError("Unsupported method")
    return response
  except FileNotFoundError:
    return "Image not found", 404

def start(port: int):
  app.run(debug=True, port=port)

if __name__ == "__main__":
  start(5000)