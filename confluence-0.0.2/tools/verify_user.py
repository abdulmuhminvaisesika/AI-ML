from collections.abc import Generator
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

from tools.auth import auth
from atlassian.confluence import Confluence


class VerifyUserTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        Verify if the given username and email correspond to a valid Confluence user.
        """
        username = tool_parameters.get("username")
        email = tool_parameters.get("user_email")

        if not username or not email:
            yield self.create_text_message("Missing required parameters: username and/or email")
            return

        # Step 1: Authenticate as Admin
        try:
            admin_confluence: Confluence = auth(self.runtime.credentials)
        except Exception as e:
            yield self.create_text_message(f"Admin authentication failed: {str(e)}")
            return

        # Step 2: Search by username
        try:
            search_cql = f'user.fullname~"{username}"'
            search_results = admin_confluence.get(f"/rest/api/search?cql={search_cql}")
        except Exception as e:
            yield self.create_text_message(f"Error searching for username '{username}': {str(e)}")
            return

        # Step 3: Validate username existence
        if not search_results or "results" not in search_results or not search_results["results"]:
            yield self.create_text_message("Username is invalid.")
            return

        user_result = search_results["results"][0]
        user_obj = user_result.get("user", {})  # Direct user object

        if user_obj.get("type") != "known":
            yield self.create_text_message("Username is invalid.")
            return

        # Step 3.5: Ensure display name matches the input username exactly
        display_name = user_obj.get("displayName", "")
        if display_name.strip().lower() != username.strip().lower():
            yield self.create_text_message(
                "Username is invalid."
            )
            return

        # Step 4: Check email match
        user_email = user_obj.get("email")
        if not user_email:
            yield self.create_text_message(f"No email found for username '{username}'.")
            return

        if user_email.lower() == email.lower():
            yield self.create_text_message("Credentials are valid.")
        else:
            yield self.create_text_message(
                "Email does not match the username."
            )
