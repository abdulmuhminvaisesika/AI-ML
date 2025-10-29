from collections.abc import Generator
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

from tools.auth import auth
from services.confluence_service import ConfluenceService


class SyncSpaceToDatasetTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        Sync a Confluence space into a Dify knowledge base dataset.
        """
        try:
            # Authenticate Confluence with runtime credentials
            confluence = auth(self.runtime.credentials)

            # Initialize Confluence service with credentials
            service = ConfluenceService(
                base_url=confluence.url,
                email=confluence.username,
                api_token=confluence.password
            )

            # Required parameter: space_key
            space_key = tool_parameters.get("space_key")
            if not space_key:
                yield self.create_text_message("Missing required parameter: space_key")
                return

            # Sync dataset
            dataset_id = service.get_or_create_dataset(space_key)
            service.sync_dataset(space_key)

            yield self.create_text_message(f"Dataset ID: {dataset_id}")

        except Exception as e:
            yield self.create_text_message(f"Failed to sync space: {str(e)}")
            return
