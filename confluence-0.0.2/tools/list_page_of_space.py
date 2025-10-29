from collections.abc import Generator
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

from tools.auth import auth


class ListPageofSpaceTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        List all pages in a space in Confluence.
        """
        try:
            confluence = auth(self.runtime.credentials)
            space_key = tool_parameters.get("space_key")

            if not space_key:
                yield self.create_text_message("Missing required parameter: space_key")
                return

            raw_pages = confluence.get_all_pages_from_space(
                space=space_key,
                start=0,
                limit=100,
                content_type="page"
            )

            base_url = self.runtime.credentials.get("url", "").rstrip("/")
            cleaned_pages = []

            for page in raw_pages:
                cleaned_pages.append({
                    "id": page.get("id"),
                    "title": page.get("title"),
                    "url": f"{base_url}/wiki{page.get('_links', {}).get('webui', '')}"
                })

            # Optional: Render text output for LLM or fallback UI
            text_output = "\n".join(
                [f"- [{p['title']}]({p['url']}) (ID: {p['id']})" for p in cleaned_pages]
            ) or "No pages found in this space."

            yield self.create_text_message(f"Pages in space **{space_key}**:\n\n{text_output}")
            yield self.create_json_message({"pages": cleaned_pages})

        except Exception as e:
            yield self.create_text_message(f"Error fetching pages for space '{space_key}': {str(e)}")
