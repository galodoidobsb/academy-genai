from collections.abc import Iterator
from typing import TypedDict

import mistletoe
from mistletoe.block_token import Heading
from mistletoe.markdown_renderer import MarkdownRenderer

# NOTE: This is a good chance to practice a generator design
# Instead of loading the full content in memory before returning, we can
# build a method that returns each section (heading) and its contents
# without the need to process the whole document at once
def extract_headers_mistletoe(file_path="include/data/guides/dynamic-tasks.md"):
    with open(file_path, "r", encoding="utf-8") as f:
        doc = mistletoe.Document(f)
    
    headers = []
    for token in doc.children:
        if isinstance(token, Heading):
            # Extract plain text from heading children tokens
            text = "".join(child.content for child in token.children if hasattr(child, 'content'))
            headers.append({"level": token.level, "text": text})
    print(headers)
    return headers

class MarkdownSection(TypedDict):
    """Represent one heading-based section extracted from a Markdown document.

    Attributes:
        title_index: Position of the heading in the document's top-level tokens.
        parent_index: Position of the nearest parent heading, or ``None`` when
            the heading has no parent.
        title: Plain-text content of the heading.
        reference: Markdown anchor generated from the heading title.
        text: Markdown content between this heading and the next heading.
    """

    title_index: int
    parent_index: int | None
    title: str
    reference: str
    text: str

# NOTE: Create another function to extract the initial heading text from the MD file
# We should define a fixed schema at first, so all docs have to follow it:

# ---
# title: "Datasets and data-aware scheduling in Airflow"
# description: "Using datasets to implement DAG dependencies and scheduling in Airflow."
# extra: WIP - ASK TALES
# ---


# NOTE: To use this, I'll implement the following on the consumer side:
# Retrieve related sections in the following manner:
# If level = 2, all children (recursively)
# If level <=3, all children (recursively) + all siblings (same level and same parent) + all parents (object_level < level and object level >= 2)
def extract_sections_from_markdown_v2(
    file_path: str = "include/data/guides/dynamic-tasks.md",
    encoding: str = "utf-8",
) -> Iterator[MarkdownSection]:
    """Parse a Markdown document into non-overlapping heading sections.

    A section starts at a heading and contains all subsequent Markdown blocks
    until the next heading, regardless of that heading's hierarchical level.
    Consequently, content belonging to a nested heading is not also included
    in its parent heading's section.

    Args:
        file_path: Path to the Markdown document that will be parsed.
        encoding: Character encoding used to read the document.

    Yields:
        A ``MarkdownSection`` dictionary for each heading in the document.
    """

    # The imports happen at runtime when Airflow executes the function.
    import mistletoe
    from mistletoe.block_token import Heading
    from mistletoe.markdown_renderer import MarkdownRenderer

    # The renderer converts the parsed block tokens back into Markdown text.
    with MarkdownRenderer() as renderer:
        # Mistletoe parses the complete file into a document syntax tree.
        with open(file_path, "r", encoding=encoding) as f:
            doc = mistletoe.Document(f)

        # Top-level blocks include headings, paragraphs, lists, code blocks, etc.
        for index, heading in enumerate(doc.children):
            # Only headings can start a new section.
            if not isinstance(heading, Heading):
                continue

            # A heading contains inline child tokens that make up its title.
            title = "".join(
                child.content
                for child in heading.children
                if hasattr(child, "content")
            )

            # These diagnostic prints expose the heading's parsed structure.
            for child in heading.children:
                if hasattr(child, "content"):
                    print("Index: ", index)
                    print("Heading content: ", heading.content)
                    print("Heading level: ", heading.level)
                    print("Child: ", child)
                    print("Child content: ", child.content)

            # Search backward for the closest heading above this one in the
            # hierarchy: an H2 is a parent of an H3, for example.
            parent_index: int | None = None
            for previous_index in range(index - 1, -1, -1):
                previous_token = doc.children[previous_index]
                if (
                    isinstance(previous_token, Heading)
                    and previous_token.level < heading.level
                ):
                    parent_index = previous_index
                    break

            print("Parent index: ", parent_index)

            # Render blocks after this heading, stopping at any new heading.
            # This boundary prevents parent and nested sections from overlapping.
            section: list[str] = []

            for token in doc.children[index + 1:]:
                if isinstance(token, Heading):
                    break
                section.append(renderer.render(token))

            # Yielding here makes sections available to the caller one at a time.
            md_dict: MarkdownSection = {
                "title_index": index,
                "parent_index": parent_index,
                "title": title,
                "reference": f"#{title.lower().replace(' ', '-')}",
                "text": "".join(section).strip(),
            }

            yield md_dict


if __name__ == "__main__":
    document = extract_sections_from_markdown_v2()
    for section in document:
        print(section, sep="\n")
