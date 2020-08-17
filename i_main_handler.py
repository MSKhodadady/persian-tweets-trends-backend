import json

from tornado.web import RequestHandler


class _ArgNotFound:
    pass


_ANF = _ArgNotFound()


class MainHandler(RequestHandler):
    """
    pre-provides some settings for handlers
    """
    __json_args: dict = None

    def prepare(self):
        #: sets the json (dict) of request body
        if self.request.headers.get("Content-Type", "").startswith("application/json"):
            self.__json_args = json.loads(self.request.body)
        else:
            self.__json_args = None

        #: set headers to json
        self.set_header('Content-Type', 'application/json')

        #: set cors
        self.set_header("Access-Control-Allow-Origin", "http://localhost:3000")
        self.set_header("Access-Control-Allow-Headers", "*")
        self.set_header('Access-Control-Allow-Methods', '*')

    def get_json_arg(self, arg_name: str, defaultValue = _ANF, arg_changer_callback = None):
        if arg_name in self.__json_args:
            return arg_changer_callback(self.__json_args[arg_name]) \
                if arg_changer_callback else self.__json_args[arg_name]

        if isinstance(defaultValue, _ArgNotFound):
            raise Exception(f'Json Argument Not found: {arg_name}')

        return defaultValue

    def options(self):
        # no body
        self.set_status(204)
        self.finish()
