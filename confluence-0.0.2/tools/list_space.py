from collections.abc import Generator
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

from tools.auth import auth 


class ListSpaceTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        try:
            confluence = auth(self.runtime.credentials)

            spaces_response = confluence.get_all_spaces(start=0, limit=100)
            spaces = spaces_response.get("results", [])

            cleaned_spaces = [
                {
                    "id": space.get("id"),
                    "key": space.get("key"),
                    "name": space.get("name"),
                    "type": space.get("type")
                }
                for space in spaces
            ]

            yield self.create_json_message({"spaces": cleaned_spaces})

        except Exception as e:
            # Log friendly error to Dify
            yield self.create_text_message(f"Error fetching Confluence spaces: {str(e)}")
