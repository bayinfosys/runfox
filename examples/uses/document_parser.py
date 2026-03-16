"""
worked_doc_parser -- extract structure from a markdown document in parallel.

A markdown string is passed as a workflow input. Three parallel steps
each analyse it independently and write their findings into the shared
state accumulator:

  extract_headers  -- all ATX headings with their depth
  extract_links    -- all inline links with text and URL
  count_words      -- whitespace-separated token count

Because the steps have no dependencies between them, all three are
submitted on the first advance() call. The workflow outcome is
assembled from the accumulator -- no separate merge step is needed.

This is the document enrichment pattern: a shared input, parallel
analysis, state accumulator as the result record.

Demonstrates: parallel steps over a shared workflow input,
              merge_state building a record across concurrent steps,
              {"var": "input.*"} and {"var": "state.*"} resolution.
"""

import pprint
import re

import runfox as rfx

DOCUMENT = """\
# Introduction

This is a sample document with [a link](https://example.com) and some text.

## Section One

More text here with another [reference](https://example.org) for good measure.

### Subsection

Final paragraph. No links here.
"""


SPEC = """
name: doc_parser

steps:
  - op: extract_headers
    label: extract_headers
    input:
      text: {"var": "input.text"}

  - op: extract_links
    label: extract_links
    input:
      text: {"var": "input.text"}

  - op: count_words
    label: count_words
    input:
      text: {"var": "input.text"}

outputs:
  headers:    {"var": "state.headers"}
  links:      {"var": "state.links"}
  word_count: {"var": "state.word_count"}
"""


def execute(label, inputs):
    text = inputs["text"]

    if label == "extract_headers":
        found = re.findall(r"^(#{1,6})\s+(.+)$", text, re.MULTILINE)
        return {"headers": [{"level": len(h), "text": t.strip()} for h, t in found]}

    if label == "extract_links":
        found = re.findall(r"\[([^\]]+)\]\(([^)]+)\)", text)
        return {"links": [{"text": t, "url": u} for t, u in found]}

    if label == "count_words":
        return {"word_count": len(text.split())}


backend = rfx.Backend(executor=execute)
wf = rfx.Workflow.from_yaml(SPEC, backend, inputs={"text": DOCUMENT})
result = wf.run()

pprint.pprint(result.outcome)
# {'headers':    [{'level': 1, 'text': 'Introduction'},
#                 {'level': 2, 'text': 'Section One'},
#                 {'level': 3, 'text': 'Subsection'}],
#  'links':      [{'text': 'a link',    'url': 'https://example.com'},
#                 {'text': 'reference', 'url': 'https://example.org'}],
#  'word_count': 36}
