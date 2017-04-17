'''
A filter that allows to create soft-link objects.

@author: josep sampe
'''
import os
from swift.common.utils import get_logger
from swift.common.utils import register_swift_info
from swift.common.wsgi import make_subrequest
from swift.common.swob import Request, Response


class SoftLinkMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='softlink')

        self.register_info()

    def register_info(self):
        register_swift_info('softlink')

    @property
    def is_object_move(self):
        return 'X-Link-To' in self.request.headers

    def create_link(self, link_path, dest_path, heads):
        """
        Creates a link to a actual object

        :param link_path: swift path of the link
        :param dest_path: swift path of the object to link
        :param heads: original object headers
        """
        self.logger.debug('Creating a link from %s to %s' % (link_path, dest_path))

        new_env = dict(self.request.environ)
        if 'HTTP_TRANSFER_ENCODING' in new_env.keys():
            del new_env['HTTP_TRANSFER_ENCODING']

        if 'HTTP_X_COPY_FROM' in new_env.keys():
            del new_env['HTTP_X_COPY_FROM']

        auth_token = self.request.headers.get('X-Auth-Token')

        link_path = os.path.join('/', self.api_version,
                                 self.account, link_path)

        sub_req = make_subrequest(
            new_env, 'PUT', link_path,
            headers={'X-Auth-Token': auth_token,
                     'Content-Length': 0,
                     'Content-Type': 'link',
                     'Original-Content-Length': heads["Content-Length"],
                     'X-Object-Sysmeta-Link-To': dest_path},
            swift_source='function_middleware')
        resp = sub_req.get_response(self.app)

        return resp

    def get_linked_object(self, dest_obj):
        """
        Makes a subrequest to the provided container/object
        :param dest_obj: container/object
        :return: swift.common.swob.Response Instance
        """
        dest_path = os.path.join('/', self.api_version, self.account, dest_obj)
        new_env = dict(self.request.environ)
        sub_req = make_subrequest(new_env, 'GET', dest_path,
                                  headers=self.request.headers,
                                  swift_source='softlink_middleware')

        return sub_req.get_response(self.app)

    def process_object_move_and_link(self):
        """
        Moves an object to the destination path and leaves a soft link in
        the original path.
        """
        link_path = os.path.join(self.container, self.obj)
        dest_path = self.request.headers['X-Link-To']
        if link_path != dest_path:
            response = self._verify_access(self.container, self.obj)
            headers = response.headers
            if "X-Object-Sysmeta-Link-To" not in response.headers \
                    and response.headers['Content-Type'] != 'link':
                self.request.method = 'COPY'
                self.request.headers['Destination'] = dest_path
                response = self.request.get_response(self.app)
            if response.is_success:
                response = self.create_link(self, link_path, dest_path, headers)
        else:
            msg = ("Error: Link path and destination path "
                   "cannot be the same.\n")
            response = Response(body=msg, headers={'etag': ''},
                                request=self.request)
        return response

    def __call__(self, env, start_response):
        self.request = Request(env)
        # TODO: Handle request

        # Pass on to downstream WSGI component
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def name_check_filter(app):
        return SoftLinkMiddleware(app, conf)
    return name_check_filter
