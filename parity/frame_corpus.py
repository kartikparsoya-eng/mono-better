#!/usr/bin/env python3
"""M13 corpus generator - upstream ws frames for the TS-vs-rust parse differential.

Emits `parity/coverage/frame-corpus.ndjson`, one `{"id":..., "frame":...}` per
line where `frame` is the RAW frame text. Raw text (not a parsed structure) is
the point: the divergences that hide here are lexical - an unpaired surrogate
escape, a truncated \\u, a number JS widens to Infinity - and none of them
survive a round trip through a parsed representation.

Paired with `parity/ts_frame_oracle.mts` (drives the real `upstreamSchema`) and
`packages/rust-syncer/tests/frame_parity_test.rs` (asserts rust agrees). See
`parity/ZERO-DIVERGENCE-PLAN.md` M13.
"""
import json
import os
import sys

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "frame-fixtures", "frame-corpus.ndjson")

# Raw JSON *text* for a string slot. Anything not expressible as a parsed value
# (invalid escapes) is exactly why these are text.
STRINGS = {
    "plain":             '"abc"',
    "empty":             '""',
    "lone-high-surr":    '"\\ud800"',
    "lone-low-surr":     '"\\udc00"',
    "valid-pair-escaped": '"\\ud83d\\udc4d"',
    "pair-then-lone":    '"\\ud83d\\udc4d\\ud800"',
    "nul-escape":        '"\\u0000"',
    "noncharacter":      '"\\uffff"',
    "escaped-backslash": '"\\\\ud800"',
    "escaped-quote":     '"a\\"b"',
    "escaped-solidus":   '"a\\/b"',
    "tab-newline":       '"a\\tb\\nc"',
    "truncated-escape":  '"\\u00"',       # invalid JSON on BOTH sides
    "bad-escape":        '"\\x41"',       # invalid JSON on BOTH sides
    "raw-control":       '"a\x01b"',      # literal control char: invalid JSON
    "long":              '"' + "Z" * 4096 + '"',
    "whitespace-pad":    '"  padded  "',
    "combining":         '"ÀÁ-áà-ﬁ-ß"',
    "sql-ish":           '"\'; select 1 --"',
    "like-meta":         '"%_\\\\%"',
}

# Raw JSON text for a number slot. JS and rust disagree about the edges here.
NUMBERS = {
    "zero": "0", "neg-zero": "-0", "neg-one": "-1",
    "int32-max-plus": "2147483648",
    "max-safe": "9007199254740991",
    "max-safe-plus-1": "9007199254740992",
    "max-safe-plus-2": "9007199254740993",
    "min-safe": "-9007199254740992",
    "fractional": "1.5",
    "overflow-f64": "1e309",        # JS -> Infinity
    "neg-overflow-f64": "-1e309",
    "tiny": "1e-330",               # JS -> 0
    "exp-upper": "1E5",
    "leading-plus": "+1",           # invalid JSON on BOTH sides
    "leading-zero": "01",           # invalid JSON on BOTH sides
    "hex": "0x10",                  # invalid JSON on BOTH sides
    "bare-inf": "Infinity",         # invalid JSON on BOTH sides
    "bare-nan": "NaN",              # invalid JSON on BOTH sides
}

WRONG = {
    "null": "null", "true": "true", "number": "42",
    "array": "[]", "object": "{}", "string": '"s"',
}

VALID = {
    "ping":            '["ping",{}]',
    "closeConnection": '["closeConnection",[]]',
    "deleteClients":   '["deleteClients",{"clientIDs":["c1"],"clientGroupIDs":["g1"]}]',
    "updateAuth":      '["updateAuth",{"auth":"tok"}]',
    "pull":            '["pull",{"clientGroupID":"g1","cookie":null,"requestID":"r1"}]',
    "changeDesiredQueries": '["changeDesiredQueries",{"desiredQueriesPatch":[]}]',
    "initConnection":  '["initConnection",{"desiredQueriesPatch":[]}]',
    "push":            '["push",{"clientGroupID":"g1","mutations":[],"pushVersion":1,'
                       '"schemaVersion":1,"timestamp":1,"requestID":"r1"}]',
    "inspect":         '["inspect",{"id":"i1","op":"version"}]',
    "ackMutationResponses": '["ackMutationResponses",{"lastMutationID":1}]',
}

STRING_SLOTS = {
    "updateAuth.auth":    '["updateAuth",{"auth":%s}]',
    "pull.clientGroupID": '["pull",{"clientGroupID":%s,"cookie":null,"requestID":"r1"}]',
    "pull.requestID":     '["pull",{"clientGroupID":"g1","cookie":null,"requestID":%s}]',
    "pull.cookie":        '["pull",{"clientGroupID":"g1","cookie":%s,"requestID":"r1"}]',
    "deleteClients.clientIDs0": '["deleteClients",{"clientIDs":[%s]}]',
    "initConnection.userPushURL":
        '["initConnection",{"desiredQueriesPatch":[],"userPushURL":%s}]',
    "initConnection.traceparent":
        '["initConnection",{"desiredQueriesPatch":[],"traceparent":%s}]',
    "changeDesiredQueries.traceparent":
        '["changeDesiredQueries",{"desiredQueriesPatch":[],"traceparent":%s}]',
    "inspect.id":         '["inspect",{"id":%s,"op":"version"}]',
    "push.clientGroupID": '["push",{"clientGroupID":%s,"mutations":[],"pushVersion":1,'
                          '"schemaVersion":1,"timestamp":1,"requestID":"r1"}]',
}

NUMBER_SLOTS = {
    "push.pushVersion": '["push",{"clientGroupID":"g1","mutations":[],"pushVersion":%s,'
                        '"schemaVersion":1,"timestamp":1,"requestID":"r1"}]',
    "push.timestamp":   '["push",{"clientGroupID":"g1","mutations":[],"pushVersion":1,'
                        '"schemaVersion":1,"timestamp":%s,"requestID":"r1"}]',
    "ackMutationResponses.lastMutationID": '["ackMutationResponses",{"lastMutationID":%s}]',
    "pull.cookie":      '["pull",{"clientGroupID":"g1","cookie":%s,"requestID":"r1"}]',
}

STRUCTURAL = {
    "not-json":           "{",
    "not-array-object":   '{"0":"ping"}',
    "not-array-string":   '"ping"',
    "not-array-number":   "7",
    "empty-array":        "[]",
    "one-element":        '["ping"]',
    "three-elements":     '["ping",{},{}]',
    "tag-number":         '[1,{}]',
    "tag-null":           "[null,{}]",
    "tag-array":          '[["ping"],{}]',
    "unknown-tag":        '["definitelyNotAThing",{}]',
    "tag-wrong-case":     '["Ping",{}]',
    "tag-trailing-space": '["ping ",{}]',
    "body-null":          '["ping",null]',
    "body-string":        '["ping","x"]',
    "body-array":         '["ping",[]]',
    "trailing-comma":     '["ping",{},]',
    "duplicate-key":      '["updateAuth",{"auth":"a","auth":"b"}]',
    "deep-nesting":       '["pull",' + "[" * 200 + "]" * 200 + "]",
    "bom-prefix":         '﻿["ping",{}]',
    "leading-whitespace": '  \t\n["ping",{}]',
    "trailing-garbage":   '["ping",{}]x',
    "nul-in-body-key":    '["updateAuth",{"au\\u0000th":"t"}]',
}


def main() -> int:
    rows = []

    def add(cid, frame):
        rows.append({"id": cid, "frame": frame})

    for name, frame in VALID.items():
        add("valid/" + name, frame)
    for name, frame in STRUCTURAL.items():
        add("structural/" + name, frame)

    for slot, tmpl in STRING_SLOTS.items():
        for sname, stext in STRINGS.items():
            add("string/%s/%s" % (slot, sname), tmpl % stext)
        for wname, wtext in WRONG.items():
            add("wrongtype/%s/%s" % (slot, wname), tmpl % wtext)

    for slot, tmpl in NUMBER_SLOTS.items():
        for nname, ntext in NUMBERS.items():
            add("number/%s/%s" % (slot, nname), tmpl % ntext)

    required = {
        "updateAuth": ["auth"],
        "pull": ["clientGroupID", "cookie", "requestID"],
        "changeDesiredQueries": ["desiredQueriesPatch"],
        "initConnection": ["desiredQueriesPatch"],
        "inspect": ["id", "op"],
        "push": ["clientGroupID", "mutations", "pushVersion", "schemaVersion",
                 "timestamp", "requestID"],
    }
    for msg, fields in required.items():
        base = json.loads(VALID[msg])
        for f in fields:
            missing = [base[0], {k: v for k, v in base[1].items() if k != f}]
            add("missing/%s/%s" % (msg, f), json.dumps(missing, separators=(",", ":")))
            nulled = [base[0], dict(base[1], **{f: None})]
            add("nullfield/%s/%s" % (msg, f), json.dumps(nulled, separators=(",", ":")))
        extra = [base[0], dict(base[1], unknownExtraField="x")]
        add("extrafield/%s" % msg, json.dumps(extra, separators=(",", ":")))

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    seen = set()
    with open(OUT, "w", encoding="utf-8") as fh:
        for r in rows:
            if r["id"] in seen:
                raise SystemExit("duplicate corpus id: " + r["id"])
            seen.add(r["id"])
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    print("M13 corpus: %d frames -> %s" % (len(rows), OUT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
