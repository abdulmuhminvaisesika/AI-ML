from typing import Any
from atlassian.confluence import Confluence


def auth(credential: dict[str, Any]) -> Confluence:
    """
    Authenticate to Confluence using manual credentials.
    """
    confluence = Confluence(
        url=credential.get("url"),
        username=credential.get("email"),
        password=credential.get("token"),
        cloud=True
    )
    return confluence
