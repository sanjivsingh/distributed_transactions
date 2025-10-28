class Response:

    def __init__(self, status: bool = True, message:str = ""):
        self.status = status
        self.message = message

    def __str__(self) -> str:
        return f"Request: status:{self.status}, message:{self.message}"

