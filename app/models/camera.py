class Camera:

    def __init__(
        self,
        camera_id,
        url
    ):

        self.id = camera_id
        self.url = url
        self.status = "created"
        self.frame = None