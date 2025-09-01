from mitmproxy import http
import os


def response(flow: http.HTTPFlow) -> None:
    # Filter only Carrefour and HTML responses
    if (
        "carrefour.es" in flow.request.pretty_url
        and "text/html" in flow.response.headers.get("content-type", "")
    ):
        url = flow.request.pretty_url.replace("https://", "").replace("/", "_")
        filename = f"{url[:100]}.html"  # cut to avoid too long filenames
        os.makedirs("raw_data", exist_ok=True)
        with open(filename, "wb") as f:
            f.write(flow.response.content)
        print(f"[+] Saved: {filename}")
