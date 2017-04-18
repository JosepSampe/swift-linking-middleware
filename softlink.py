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
    def is_object_link(self):
        return 'X-Link-To' in self.request.headers

    def verify_access(self, cont, obj):
        """
        Verifies access to the specified object in swift
        :param cont: swift container name
        :param obj: swift object name
        :return response: Object response
        """
        path = os.path.join('/', self.api_version, self.account, cont, obj)
        self.logger.debug('Verifying access to %s' % path)

        new_env = dict(self.req.environ)
        if 'HTTP_TRANSFER_ENCODING' in new_env.keys():
            del new_env['HTTP_TRANSFER_ENCODING']

        auth_token = self.req.headers.get('X-Auth-Token')
        sub_req = make_subrequest(new_env, 'HEAD', path,
                                  headers={'X-Auth-Token': auth_token},
                                  swift_source='softlink_middleware')

        return sub_req.get_response(self.app)

    def create_link(self, link_path, dest_path, heads):
        """
        Creates a link to a actual object

        :param link_path: swift path of the link
        :param dest_path: swift path of the object to link
        :param heads: original object headers
        """
        self.logger.debug('Creating a link from %s to %s' %
                          (link_path, dest_path))

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
                    swift_source='softlink_middleware')
        resp = sub_req.get_response(self.app)

        return resp

    def get_linked_object(self, dest_obj):
        """
        Makes a subrequest to the provided container/object
        :param dest_obj: container/object
        :return: swift.common.swob.Response Instance
        """
        dest_path = os.path.join('/', self.api_version, self.account,
                                 dest_obj)
        new_env = dict(self.req.environ)
        sub_req = make_subrequest(new_env, 'GET', dest_path,
                                  headers=self.req.headers,
                                  swift_source='softlink_middleware')

        return sub_req.get_response(self.app)

    def process_object_link(self):
        """
        Moves an object to the destination path and leaves a soft link in
        the original path.
        """
        link_path = os.path.join(self.container, self.obj)
        dest_path = self.req.headers['X-Link-To']
        if link_path != dest_path:
            resp = self.verify_access(self.container, self.obj)
            if resp.is_success:
                headers = resp.headers
                if "X-Object-Sysmeta-Link-To" not in resp.headers \
                        and resp.headers['Content-Type'] != 'link':
                    self.req.method = 'COPY'
                    self.req.headers['Destination'] = dest_path
                    response = self.req.get_response(self.app)
                if response.is_success:
                    response = self.create_link(self, link_path, dest_path,
                                                headers)
            else:
                msg = ("Error: The main object does not exists in Swift.\n")
                response = Response(body=msg, headers={'etag': ''},
                                    request=self.req)
        else:
            msg = ("Error: Link path and destination path "
                   "cannot be the same.\n")
            response = Response(body=msg, headers={'etag': ''},
                                request=self.req)
        return response

    def __call__(self, env, start_response):
        self.req = Request(env)
        if self.req.method == 'GET':
            resp = self.app(env, start_response)
            if "X-Object-Sysmeta-Link-To" in resp.headers:
                dest_obj = resp.headers["X-Object-Sysmeta-Link-To"]
                return self.get_linked_object(dest_obj)
        if self.req.method == 'POST':
            if self.is_object_link:
                resp = self.process_object_link()
                return resp(env, start_response)
        else:
            # Pass on to downstream WSGI component
            return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def softlink_filter(app):
        return SoftLinkMiddleware(app, conf)
    return softlink_filter
