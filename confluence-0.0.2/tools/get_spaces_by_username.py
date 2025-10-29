from collections.abc import Generator
from typing import Any, Dict, List
from urllib.parse import quote_plus
import json

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

from tools.auth import auth
from atlassian.confluence import Confluence


class GetSpacesByUsernameTool(Tool):
    """
    Get all Confluence spaces a given user has access to (direct user or via group).
    Input:
      - username (accountId, displayName, or email)
    Uses runtime credentials via tools.auth(auth(self.runtime.credentials)).
    """

    GROUP_PAGE_LIMIT = 50
    SPACE_PAGE_LIMIT = 200

    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        username = tool_parameters.get("username")
        if not username:
            yield self.create_text_message("Missing required parameter: username")
            return

        # Step 1: Authenticate as Admin using runtime credentials
        try:
            admin_confluence: Confluence = auth(self.runtime.credentials)
        except Exception as e:
            yield self.create_text_message(f"Admin authentication failed: {str(e)}")
            return

        # Helper: normalize identifier for display/email compare
        ident = username
        ident_lower = ident.strip().lower()

        # cache group_name -> dict[accountId] -> {"displayName":..., "email":...}
        group_cache: Dict[str, Dict[str, Dict[str, str]]] = {}

        def fetch_group_members(group_name: str) -> Dict[str, Dict[str, str]]:
            """Fetch human members (paginated) for a group and return mapping accountId -> {displayName, email}."""
            if group_name in group_cache:
                return group_cache[group_name]

            members: Dict[str, Dict[str, str]] = {}
            start = 0
            limit = self.GROUP_PAGE_LIMIT
            # group_name needs to be path-encoded
            encoded = quote_plus(group_name)
            while True:
                path = f"/rest/api/group/{encoded}/member?start={start}&limit={limit}"
                try:
                    resp = admin_confluence.get(path)
                except Exception:
                    # on any failure, stop and return what we have
                    break
                if not resp or "results" not in resp:
                    break
                for u in resp.get("results", []):
                    account_id = u.get("accountId")
                    display_name = u.get("displayName")
                    account_type = u.get("accountType")
                    email_addr = u.get("email") or None
                    if account_id and display_name and account_type == "atlassian":
                        members[account_id] = {"displayName": display_name, "email": email_addr}
                # paging control
                size = resp.get("size", 0)
                start = resp.get("start", start) + size if "start" in resp else start + size
                total = resp.get("total", None)
                # break conditions
                if total is not None:
                    if start >= total:
                        break
                else:
                    # if server didn't provide total, stop when fewer than limit returned
                    if size < limit:
                        break
            group_cache[group_name] = members
            return members

        matched_spaces: List[str] = []

        # Step 2: iterate spaces (with pagination)
        start = 0
        limit = self.SPACE_PAGE_LIMIT
        while True:
            space_path = f"/rest/api/space?expand=permissions&start={start}&limit={limit}"
            try:
                spaces_resp = admin_confluence.get(space_path)
            except Exception as e:
                yield self.create_text_message(f"Failed to fetch spaces: {str(e)}")
                return

            if not spaces_resp or "results" not in spaces_resp:
                break

            spaces = spaces_resp.get("results", [])
            # For each space, check direct users first, then groups
            for space in spaces:
                space_key = space.get("key")
                permissions = space.get("permissions", []) or []

                # flag to short-circuit if user already matched for this space
                matched_here = False
                # collect unique group names for this space (to avoid duplicate fetches inside the space)
                group_names = set()

                # Check direct user subjects and collect groups
                for perm in permissions:
                    users = perm.get("subjects", {}).get("user", {}).get("results", []) or []
                    for u in users:
                        account_id = u.get("accountId")
                        display_name = (u.get("displayName") or "").strip()
                        email_addr = (u.get("email") or "") or ""
                        # exact accountId match
                        if account_id and account_id == ident:
                            matched_here = True
                            break
                        # displayName or email (case-insensitive exact)
                        if display_name and display_name.lower() == ident_lower:
                            matched_here = True
                            break
                        if email_addr and email_addr.lower() == ident_lower:
                            matched_here = True
                            break
                    if matched_here:
                        break

                    # collect group names
                    groups = perm.get("subjects", {}).get("group", {}).get("results", []) or []
                    for g in groups:
                        gname = g.get("name")
                        if gname:
                            group_names.add(gname)

                if matched_here:
                    matched_spaces.append(space_key)
                    continue  # next space

                # If not matched directly, check group members (fetch each group once)
                for gname in group_names:
                    members = fetch_group_members(gname)
                    # check members for match
                    for account_id, info in members.items():
                        display_name = (info.get("displayName") or "").strip()
                        email_addr = (info.get("email") or "") or ""
                        if account_id and account_id == ident:
                            matched_here = True
                            break
                        if display_name and display_name.lower() == ident_lower:
                            matched_here = True
                            break
                        if email_addr and email_addr.lower() == ident_lower:
                            matched_here = True
                            break
                    if matched_here:
                        matched_spaces.append(space_key)
                        break  # stop checking further groups for this space

            # pagination control for spaces
            size = spaces_resp.get("size", 0)
            # advance start if available
            if "start" in spaces_resp and "size" in spaces_resp:
                start = spaces_resp["start"] + spaces_resp["size"]
                if spaces_resp.get("size", 0) == 0:
                    break
            else:
                # fallback: if fewer than limit returned, assume done
                if size < limit:
                    break
                start += size

            # also stop when no next link
            if not spaces_resp.get("_links", {}).get("next") and size < limit:
                break

        # Final result
        seen = set()
        ordered_unique_spaces = []
        for s in matched_spaces:
            if s not in seen:
                seen.add(s)
                ordered_unique_spaces.append(s)

        result_str = ", ".join(ordered_unique_spaces)
        yield self.create_text_message(result_str)