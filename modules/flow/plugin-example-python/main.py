"""The example plugin in Python: ``pyexample.count-words@1`` and ``pyexample.sign@1``.

To install it, copy this directory to the plugins directory together with ``xflow_plugin.py``
(``modules/flow/plugin-sdk/python``).
"""

import hashlib
import hmac
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "plugin-sdk", "python"))

from xflow_plugin import Plugin, failed, field, ok, output  # noqa: E402

plugin = Plugin()

plugin.connection_kind("pyexample", "Signing key", [field("key", secret=True, description="The key of the HMAC")])


@plugin.node(
    "count-words",
    label="Count words",
    category="Python",
    config=[field("text", description="The text to count")],
    outputs=[output("words", "num")],
)
def count_words(params, input, connection):
    return ok(words=len(str(params.get("text", "")).split()))


@plugin.node(
    "sign",
    label="Sign",
    category="Python",
    config=[field("connection", description="A connection of kind pyexample"), field("text")],
    outputs=[output("signature", "str")],
)
def sign(params, input, connection):
    if connection is None:
        return failed("no connection")
    key = connection["secrets"]["key"].encode()
    return ok(signature=hmac.new(key, params["text"].encode(), hashlib.sha256).hexdigest())


plugin.serve()
