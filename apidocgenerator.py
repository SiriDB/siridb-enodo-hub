import re
from aiohttp import web
import aiohttp_cors

from lib.webserver.routes import setup_routes


def generate_api_docs(app):
    mdstring = ""
    mdstring += "|endpoint|method|description|args|\n"
    mdstring += "|--------|------|-----------|----|\n"
    for route in list(app.router.routes()):
        if hasattr(route.handler.__self__, "__name__") and \
                route.handler.__self__.__name__ == "ApiHandlers":
            docs = route.handler.__doc__
            uri = ""
            method = route.method
            if hasattr(route.resource, "_path"):
                uri = route.resource._path
            elif hasattr(route.resource, "_formatter"):
                uri = route.resource._formatter

            if method != "HEAD":
                desc = re.search(
                    r"\A(.*?)\n\n", docs, re.MULTILINE | re.DOTALL)
                # TODO: Multiline query args support
                matches = re.search(
                    r"(?:(?:\s{,8}Query args)|(?:\s{,8}JSON POST data))"
                    r":\n(.*?)(?=(?:\n{2,})|(?:\s{,8}\n))",
                    docs, re.MULTILINE | re.DOTALL)
                if desc:
                    desc = desc.group(0)
                if matches:
                    matches = [
                        matches.group(groupNum)
                        for groupNum in range(0, len(matches.groups()))]
                argdocs = ""
                if matches:
                    for m in matches:
                        argdocs += m

                desc = desc.replace("\n", "")
                argdocs = argdocs.replace("\n", "")
                mdstring += f"|{uri}|{method}|{desc}|{argdocs}|\n"
    with open('restapi.md', 'w') as f:
        f.write(mdstring)
    f.close()


if __name__ == '__main__':
    app = web.Application()
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
        )
    })
    setup_routes(app, cors)
    generate_api_docs(app)
