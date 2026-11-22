"""Generates the site pages from the Markdown files of the repository.

README.md, COMPONENTS.md and the other documents stay the single source of
truth and keep working when browsed on GitHub. Run this script before
`zensical build` or `zensical serve` (Zensical has no hooks, so the pages
cannot be generated during the build):

    python docs/generate.py
    zensical build -f docs/zensical.toml

It splits the documents into pages of the site along their headings, drops
the emojis in front of the headings and the horizontal rules between
sections, and rewrites their links: links to a section point to the page it
ended up on, links to docs/assets/ to the site's assets, and links to any
other file of the repository to the file on GitHub. The generated pages
and the copied assets are written to content/ and gitignored.

The Releases page comes from the git history: git-cliff (see cliff.toml)
writes the notes of every version tag, the same the GitHub Release of the
tag gets. It needs the full history with the tags, not a shallow clone.
"""

import posixpath
import re
import shutil
import subprocess
import unicodedata
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / "docs"
CONTENT = DOCS / "content"
ASSETS = DOCS / "assets"
SOURCE_URL = "https://github.com/GO-TRAM/TRAM/blob/main/"

GETTING_STARTED = "guide/getting-started.md"
STREAMS = "guide/streams.md"
API = "guide/api.md"
COMPONENTS_INDEX = "components/index.md"
RELEASES = "releases.md"

# Title of every generated page, in the order of the navigation.
TITLES = {
    GETTING_STARTED: "Getting Started",
    STREAMS: "Streams",
    API: "HTTP API",
    "guide/configuration.md": "Configuration Sources",
    "guide/metrics.md": "Metrics",
    "guide/security.md": "Security",
    COMPONENTS_INDEX: "Overview",
    "components/sources.md": "Sources",
    "components/stages.md": "Stages",
    "components/sinks.md": "Sinks",
    "kubernetes.md": "Kubernetes Operator",
    "benchmarks.md": "Benchmarks",
    "contributing.md": "Contributing",
}

# Where the sections of each document go, by heading without its emoji.
# A section takes its subsections along unless they are listed themselves;
# None drops it. Its heading becomes a second-level heading of the page, or,
# with UNWRAP, is dropped and its subsections take its place.
UNWRAP = True
SECTIONS = {
    "README.md": {
        "TRAM: A Lightweight, Extensible Data Pipeline Framework": None,
        "Quick Start": (GETTING_STARTED, UNWRAP),
        "API Endpoints": (API, False),
        "Example API Usage": (API, UNWRAP),
        "Metrics": ("guide/metrics.md", UNWRAP),
        "Core Concepts": (STREAMS, UNWRAP),
        "Available Components": (STREAMS, False),
        "Advanced Configuration": (STREAMS, UNWRAP),
        "Updating a Stream": (STREAMS, False),
        "Testing a Stream": (STREAMS, False),
        "Configuration Sources": ("guide/configuration.md", UNWRAP),
        "Security": ("guide/security.md", UNWRAP),
    },
    "COMPONENTS.md": {
        "Components Documentation": (COMPONENTS_INDEX, UNWRAP),
        "Sources": ("components/sources.md", UNWRAP),
        "Available Sources": (COMPONENTS_INDEX, False),
        "Stages": ("components/stages.md", UNWRAP),
        "Available Stages": (COMPONENTS_INDEX, False),
        "Sinks": ("components/sinks.md", UNWRAP),
        "Available Sinks": (COMPONENTS_INDEX, False),
        "Notes": (COMPONENTS_INDEX, False),
    },
    "internal/adapters/k8scrd/README.md": {
        "Kubernetes Operator": ("kubernetes.md", UNWRAP),
    },
    "BENCHMARK.md": {
        "Benchmarks": ("benchmarks.md", UNWRAP),
    },
    "CONTRIBUTING.md": {
        "Contributing to TRAM": ("contributing.md", UNWRAP),
        "Table of Contents": None,
    },
}

HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
EMOJI = re.compile(r"^[^\w\s`*\[(]+\s*")
RULE = re.compile(r"^\s*(-{3,}|\*{3,}|_{3,})\s*$")
FENCE = re.compile(r"^\s*(```|~~~)")
LINK = re.compile(r"(!?\[[^\]]*\]\()([^)\s]+)(\))")


def slugify(value, separator="-"):
    """The slug MkDocs and Zensical give a heading, for links to its anchor."""
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    return re.sub(r"[%s\s]+" % separator, separator, value)


def split_all():
    """Returns the parts of the documents per page, and for every (document,
    anchor) the page it is on and whether the page has the anchor."""
    chunks, anchors = defaultdict(list), {}
    for source, sections in SECTIONS.items():
        text = (REPO / source).read_text(encoding="utf-8")
        for page, part in split(source, text, sections, anchors):
            chunks[page].append((source, part))
    return chunks, anchors


class Splitter:
    """Collects the parts of one document per page while reading its lines."""

    def __init__(self, source, sections, anchors):
        self.source, self.sections, self.anchors = source, sections, anchors
        # Stack of enclosing sections: (level, page, shift of heading levels).
        self.stack, self.page, self.shift = [], None, 0
        self.current, self.parts = [], []
        self.seen = set()

    def flush(self):
        if self.page and self.current:
            self.parts.append((self.page, "".join(self.current)))
        self.current.clear()

    def place(self, level, title):
        """Returns the page of a heading, the shift of its heading levels and
        whether it is unwrapped."""
        if title in self.sections:
            self.seen.add(title)
            target = self.sections[title]
            page, unwrap = target if target else (None, False)
            return page, (1 if unwrap else 2) - level, unwrap
        if self.stack:
            return self.stack[-1][1], self.stack[-1][2], False
        return None, 0, False

    def heading(self, level, title):
        while self.stack and self.stack[-1][0] >= level:
            self.stack.pop()
        page, shift, unwrap = self.place(level, title)
        self.stack.append((level, page, shift))

        if page != self.page:
            self.flush()
            self.page = page
        self.shift = shift
        if not self.page:
            return
        # An unwrapped heading leaves no anchor; links to it go to the page.
        self.anchors.setdefault((self.source, slugify(title)), (self.page, not unwrap))
        if not unwrap:
            self.current.append("#" * (level + self.shift) + " " + title + "\n")


def split(source, text, sections, anchors):
    splitter, in_fence = Splitter(source, sections, anchors), False
    for line in text.splitlines(keepends=True):
        heading = None if in_fence else HEADING.match(line)
        if FENCE.match(line):
            in_fence = not in_fence
        elif heading:
            splitter.heading(len(heading[1]), EMOJI.sub("", heading[2]))
            continue
        elif RULE.match(line):
            continue
        splitter.current.append(line)
    splitter.flush()
    # A heading renamed in the document would silently drop its section.
    missing = sections.keys() - splitter.seen
    if missing:
        raise SystemExit(f"{source} has no heading {', '.join(sorted(missing))}; update SECTIONS in docs/generate.py")
    return splitter.parts


def rewrite(source, page, text, anchors):
    lines, in_fence = [], False
    for line in text.splitlines(keepends=True):
        if FENCE.match(line):
            in_fence = not in_fence
        elif not in_fence:
            line = LINK.sub(lambda m: m[1] + target(source, page, m[2], anchors) + m[3], line)
        lines.append(line)
    return "".join(lines)


def target(source, page, link, anchors):
    if "://" in link or link.startswith("mailto:"):
        return link

    path, _, anchor = link.partition("#")
    # GitHub keeps the dash an emoji at the start of a heading leaves in
    # its anchor (#-security); the site drops it (#security).
    anchor = anchor.lstrip("-")
    path = posixpath.normpath(posixpath.join(posixpath.dirname(source), path)) if path else source

    if path in SECTIONS:
        dest, keep = anchors.get((path, anchor), (first_page(path), False))
        if not keep:
            anchor = ""
    elif path.startswith("docs/assets/"):
        dest = path.removeprefix("docs/")
    else:
        return SOURCE_URL + path + ("#" + anchor if anchor else "")

    suffix = "#" + anchor if anchor else ""
    if dest == page:
        return suffix or "#"
    return posixpath.relpath(dest, posixpath.dirname(page)) + suffix


def first_page(source):
    return next(target[0] for target in SECTIONS[source].values() if target)


def releases():
    """The Releases page: the notes of every version tag, by git-cliff."""
    git = ["git", "-C", str(REPO)]
    shallow = subprocess.run(
        git + ["rev-parse", "--is-shallow-repository"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    if shallow == "true":
        raise SystemExit("the Releases page needs the full history: git fetch --unshallow --tags")
    if shutil.which("git-cliff") is None:
        raise SystemExit("git-cliff not found: pip install -r docs/requirements.txt")
    return subprocess.run(
        ["git-cliff", "--config", str(REPO / "cliff.toml"), "--repository", str(REPO)],
        check=True, capture_output=True, text=True,
    ).stdout


def clean():
    for page in [*TITLES, RELEASES]:
        (CONTENT / page).unlink(missing_ok=True)
    shutil.rmtree(CONTENT / "assets", ignore_errors=True)


def main():
    chunks, anchors = split_all()
    clean()
    empty = [page for page in TITLES if not chunks[page]]
    if empty:
        raise SystemExit(f"no section goes to {', '.join(empty)}; check SECTIONS in docs/generate.py")
    for page, title in TITLES.items():
        content = f"# {title}\n\n" + "".join(
            rewrite(source, page, text, anchors) for source, text in chunks[page]
        )
        destination = CONTENT / page
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content, encoding="utf-8")
        print(f"generated content/{page}")
    (CONTENT / RELEASES).write_text(releases(), encoding="utf-8")
    print(f"generated content/{RELEASES}")
    assets = CONTENT / "assets"
    shutil.copytree(ASSETS, assets)
    print(f"copied assets/ to content/assets/ ({len(list(ASSETS.rglob('*')))} entries)")


if __name__ == "__main__":
    main()
