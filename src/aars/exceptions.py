"""
Types of exceptions that can be raised when using the AARS library.
All exceptions are subclasses of [AlephError][aars.exceptions.AlephError].
"""


class AlephError(Exception):
    """Base class for exceptions in this module."""

    pass


class AlephPermissionError(AlephError):
    """Exception raised when a user is not authorized to perform an action on an item."""

    def __init__(
        self,
        user_address: str,
        item_hash: str,
        item_owner: str,
        message="User {0} is not authorized to modify {1} by user {2}",
    ):
        self.user_address = user_address
        self.item_hash = item_hash
        self.item_owner = item_owner
        self.message = message.format(
            self.user_address, self.item_hash, self.item_owner
        )
        super().__init__(self.message)


class AlreadyUsedError(AlephError):
    """Exception raised when a PageableResponse has already been used."""

    def __init__(
        self,
        message="PageableResponse has already been iterated over. It is recommended to "
        "to store the result of all() or page() or to create a new query.",
    ):
        self.message = message
        super().__init__(self.message)


class AlreadyForgottenError(AlephError):
    """
    Exception raised when a user tries to forget an item that has already been forgotten.
    """

    def __init__(
        self,
        content,
        message="Object '{0}' has already been forgotten. It is recommended to delete the "
        "called object locally.",
    ):
        self.item_hash = content.item_hash
        self.message = f"{message.format(self.item_hash)}"
        super().__init__(self.message)


class PostTypeIsNoClassError(AlephError):
    """Exception raised when a received post_type is not resolvable to any python class in current runtime."""

    def __init__(
        self,
        content,
        message="Received post_type '{0}' from channel '{1}' does not currently exist as a "
        "class.",
    ):
        self.post_type = content["type"]
        self.content = content["content"]
        self.channel = content["channel"]
        self.message = f"""{message.format(self.post_type, self.channel)}\n
        Response of {self.post_type} provides the following fields:\n
        {[key for key in self.content.keys()]}"""
        super().__init__(self.message)


class InvalidMessageTypeError(AlephError):
    """Exception raised when program received a different message type than expected."""

    def __init__(
        self,
        received,
        expected,
        message="Expected message type '{0}' but actually received '{1}'",
    ):
        self.received = received
        self.expected = expected
        self.message = f"{message.format(self.expected, self.received)}"
        super().__init__(self.message)


class NotStoredError(AlephError):
    """Exception raised when a requested object is not stored on Aleph and has no `item_hash`."""

    def __init__(
        self,
        record,
        message="Record '{0}'\nis not stored on Aleph. It is required to store the "
        "record with .save() before calling this method.",
    ):
        self.type = record.content
        self.message = f"{message.format(self.type)}"
        super().__init__(self.message)


class AlephPostError(AlephError):
    """Exception raised when a network request to Aleph fails."""

    def __init__(
        self,
        obj,
        response_code,
        response,
        message="Failed to post object '{0}' to Aleph with status {1}: {2}",
    ):
        self.obj = obj
        self.response_code = response_code
        self.response = response
        self.message = f"{message.format(self.obj, self.response_code, self.response)}"
        super().__init__(self.message)
