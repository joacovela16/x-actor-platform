"""SDK for x-flow plugins written in Python: one file, only the standard library.

A plugin is a process that the server starts with the manifest's ``command`` and that speaks JSON-RPC 2.0 over stdio, one
message per line. This module speaks it for you::

    from xflow_plugin import Plugin, field, output, ok, failed

    plugin = Plugin()

    @plugin.node("count-words", label="Count words", config=[field("text", "str")], outputs=[output("count", "num")])
    def count_words(params, input, connection):
        return ok(count=len(params["text"].split()))

    plugin.serve()

``params`` arrive with their ``${state.x}`` references already resolved. ``input`` is the whole node input (``state``,
``memory``, ``nodeId``, ``now``...). ``connection`` is the connection named by the ``connection`` param, with its secrets,
or ``None``. Log with ``plugin.log`` or ``print(..., file=sys.stderr)``: stdout belongs to the protocol.
"""

import json
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor

__all__ = ["Plugin", "field", "output", "port", "connection_kind", "ok", "failed", "via", "dtype"]


def dtype(name, **more):
    """A data type: ``"str"``, ``"num"``, ``"bool"``, ``"any"``, ``{"type": "list", "element": ...}``..."""
    if isinstance(name, dict):
        return name
    return dict({"type": name}, **more)


def field(name, type="str", required=True, default=None, secret=False, description=None, one_of=None, ui=None):
    f = {"name": name, "type": dtype(type), "required": required, "secret": secret}
    if default is not None:
        f["default"] = default
    if description is not None:
        f["description"] = description
    if one_of:
        f["oneOf"] = list(one_of)
    if ui is not None:
        f["ui"] = ui
    return f


def output(name, type="any", target=None):
    """A value the node writes; ``target`` is the path in the state, by default its name."""
    return {"name": name, "target": target or name, "type": dtype(type)}


def port(name, label=None):
    p = {"name": name}
    if label is not None:
        p["label"] = label
    return p


def connection_kind(kind, label, config):
    return {"kind": kind, "label": label, "config": {"fields": list(config)}}


def ok(values=None, port="out", **more):
    """The node finished well: it writes ``values`` (or the keyword arguments) and goes on through ``port``."""
    return {"values": dict(values or {}, **more), "port": port}


def via(port):
    return {"values": {}, "port": port}


def failed(reason):
    return {"port": "error", "error": str(reason)}


class Plugin:
    def __init__(self, workers=8):
        self._nodes = {}
        self._connection_kinds = []
        self._out = sys.stdout
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=workers)
        self.name = None

    def node(self, kind, label=None, category=None, config=(), outputs=(), ports=None, side_effects="pure"):
        """Declares a node; the decorated function gets ``(params, input, connection)`` and returns an output."""

        def register(fn):
            d = {
                "kind": kind,
                "label": label or kind,
                "config": {"fields": list(config)},
                "sideEffects": side_effects,
                "outputs": list(outputs),
            }
            if category:
                d["category"] = category
            if ports is not None:
                d["ports"] = list(ports)
            self._nodes[kind] = (d, fn)
            return fn

        return register

    def connection_kind(self, kind, label, config):
        """Declares a kind of connection; it must be the plugin's name or start with ``<name>.``."""
        self._connection_kinds.append(connection_kind(kind, label, config))

    def log(self, *args):
        print(*args, file=sys.stderr, flush=True)

    def _send(self, message):
        line = json.dumps(message, separators=(",", ":"))
        with self._lock:
            self._out.write(line + "\n")
            self._out.flush()

    def _answer(self, id, result=None, error=None):
        if error is not None:
            self._send({"jsonrpc": "2.0", "id": id, "error": {"code": -32000, "message": error}})
        else:
            self._send({"jsonrpc": "2.0", "id": id, "result": result})

    def _run(self, id, params):
        kind = params.get("kind")
        entry = self._nodes.get(kind)
        if entry is None:
            self._answer(id, error="unknown kind '%s'" % kind)
            return
        node_input = params.get("input") or {}
        try:
            out = entry[1](node_input.get("params") or {}, node_input, params.get("connection"))
            self._answer(id, out if out is not None else ok())
        except Exception as e:  # a node that throws fails, with the reason
            self.log(traceback.format_exc())
            self._answer(id, failed("%s: %s" % (type(e).__name__, e)))

    def serve(self):
        """Answers the server until it says ``shutdown`` or closes stdin."""
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except ValueError:
                self.log("not JSON: " + line)
                continue
            method, id, params = message.get("method"), message.get("id"), message.get("params") or {}
            if method == "initialize":
                self.name = (params.get("plugin") or {}).get("name")
                nodes = [d for d, _ in self._nodes.values()]
                self._answer(id, {"nodes": nodes, "connectionKinds": self._connection_kinds})
            elif method == "run":
                self._pool.submit(self._run, id, params)
            elif method == "shutdown":
                break
            elif method == "$/cancel":
                pass  # the answer, if it comes, is ignored
            elif id is not None:
                self._answer(id, error="unknown method '%s'" % method)
        self._pool.shutdown(wait=False)
