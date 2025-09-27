import traceback

# Define Exception IDs
EXC_ID_STREAM_UNABLE_TO_EXTRACT             = 0x0000
EXC_ID_API_VALIDATION_FAILED                = 0x0001
EXC_ID_STREAM_UNABLE_TO_ENCODE              = 0x0002
EXC_ID_INVALID_STATE                        = 0x0003
EXC_ID_INVALID_PARAM                        = 0x0004
EXC_ID_INVALID_LENGTH                       = 0x0005
EXC_ID_INVALID_VALUE                        = 0x0006

class XBase(Exception):
    
    def __init__(self, id:int, message: str=None, data: bytes=None):
        super().__init__(message)

        self.id = id

        self.messages = []
        if message is not None:
            self.messages.append(message)

        self.data = []
        if data is not None:
            self.data.append(data)
            
    def __str__(self):
        return f"XBase(id={self.id}, messages={self.messages}, data={[len(d) for d in self.data]})\n {traceback.format_exc()}"

class XStreamUnableToExtract(XBase):

    def __init__(self, message: str=None, data: bytes=None):
        super().__init__(EXC_ID_STREAM_UNABLE_TO_EXTRACT, message, data)

class XStreamUnableToEncode(XBase):

    def __init__(self, message: str=None, data: bytes=None):
        super().__init__(EXC_ID_STREAM_UNABLE_TO_ENCODE, message, data)

class XAPIValidationFailed(XBase):

    def __init__(self, message: str=None, data: bytes=None):
        super().__init__(EXC_ID_API_VALIDATION_FAILED, message, data)